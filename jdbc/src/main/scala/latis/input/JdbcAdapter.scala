package latis.input

import java.net.URI
import java.sql.PreparedStatement
import java.sql.ResultSet

import cats.effect.IO
import cats.syntax.all._
import doobie._
import doobie.implicits._
import doobie.util.stream.repeatEvalChunks
import fs2.Stream
import fs2.Stream.bracket
import fs2.Stream.eval

import latis.data._
import latis.data.Data._
import latis.model._
import latis.ops._
import latis.util.ConfigLike
import latis.util.LatisException
import latis.util.StreamUtils
import latis.util.StreamUtils.contextShift
import latis.util.dap2.parser.ast
import latis.util.SqlBuilder

/**
 * Defines an adapter for JDBC-supported databases.
 */
case class JdbcAdapter(
  model: DataType,
  config: JdbcAdapter.Config
) extends Adapter {
  //TODO: ensure that there are no nested functions, doesn't make sense for database table
  //TODO: consider lifecycle of database connections, when are they released, use connection pool
  //TODO: Support timestamp (and other non-numeric time) database column type.
  //      The query builder can handle this but not the result parsing.
  //TODO: disallow custom ordering for JDBC datasets, except descending

  override def canHandleOperation(op: Operation): Boolean = op match {
    // Assumes operation has already been deemed valid for the model
    case Selection(_, sop, _) => sop match {
      case ast.Tilde   => false //can't support nearest neighbor
      case ast.EqTilde | ast.NeEqTilde => false //TODO: support sql "like"
      case _           => true
    }
    case _: Projection => true
    case _: Rename     => true
    case _: Head       => true
    case _: Take       => true
    //TODO: last, takeRight: order by desc, limit
    //TODO: drop with "offset"
    case _             => false
  }

  /**
   * Gets the projected domain and range Scalars.
   *
   * This will apply all operations to the model to ensure we get the correct
   * set of projected and renamed variables.
   * Since this delegates to the Projection operation,
   * the order of variables in the model will be preserved as opposed to using
   * the order of the variables in the projection operation.
   *
   * If no Projection operation is provided, this will include all scalar
   * variables excluding Index variables since they are not explicitly
   * represented in the data.
   */
  private def getProjectedScalars(ops: Seq[UnaryOperation]): (Seq[Scalar], Seq[Scalar]) = {
    ops.foldLeft(model) { (mod, op) =>
      op.applyToModel(mod).fold(throw _, identity)
    } match {
      case Function(d, r) => (d.getScalars.filterNot(_.isInstanceOf[Index]), r.getScalars)
      case dt => (List(), dt.getScalars)
    }
  }

  /** Combines the domain and range Scalars. */
  private def getAllProjectedScalars(ops: Seq[UnaryOperation]): Seq[Scalar] =
    getProjectedScalars(ops) match {
      case (ds, rs) => ds ++ rs
    }

  /** Count the number of projected domain variables. */
  private def getProjectedDomainCount(ops: Seq[UnaryOperation]): Int =
    getProjectedScalars(ops) match {
      case (ds, _) => ds.length
    }

  /**
   * Defines the JDBC request fetch size.
   */
  private[input] val fetchSize: Int = 100
  //TODO: get from config
  //TODO: is 100 (from Ryan's original code) a reasonable fetchSize?
  // We use 10000 for MMS and MAVEN WebTCAD
  // "By default, most JDBC drivers use a fetch size of 10."


  def getData(baseUri: URI, ops: Seq[Operation]): Data = {
    val uops: List[UnaryOperation] = ops.toList.collect {
      case uop: UnaryOperation => uop
    }

    // This escapes to raw JDBC for efficiency.
    def getNextChunkSamples(chunkSize: Int): ResultSetIO[Seq[Sample]] =
      FRS.raw { rs =>
        var n            = chunkSize
        val rowBuilder   = Vector.newBuilder[Seq[Datum]]
        val datumBuilder = Vector.newBuilder[Datum]
        while (n > 0 && rs.next) {
          getAllProjectedScalars(uops).zipWithIndex.foreach {
            case (scalar, index) =>
              val colIndex = index + 1 //ResultSet uses 1-based index
              val datum: Datum = scalar.valueType match {
                //TODO: deal with java.sql.Types.TIMESTAMP
                //      rs.getTimestamp(name, gmtCalendar).getTime will be native ms
                case BooleanValueType =>
                  val v = rs.getBoolean(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else BooleanValue(v)
                case ByteValueType =>
                  val v = rs.getByte(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else ByteValue(v)
                case CharValueType =>
                  val v = rs.getByte(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else CharValue(v.toChar) //TODO: but java char is 2 bytes
                case ShortValueType =>
                  val v = rs.getShort(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else ShortValue(v)
                case IntValueType =>
                  val v = rs.getInt(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else IntValue(v)
                case LongValueType =>
                  val v = rs.getLong(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else LongValue(v)
                case FloatValueType =>
                  val v = rs.getFloat(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else FloatValue(v)
                case DoubleValueType =>
                  val v = rs.getDouble(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else DoubleValue(v)
                case BinaryValueType =>
                  val v = rs.getBytes(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else BinaryValue(v)
                case StringValueType =>
                  val v = rs.getString(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else StringValue(v)
                case BigIntValueType =>
                  //Use Long in lieu of direct support for BigInt
                  val v = rs.getLong(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else BigIntValue(v)
                case BigDecimalValueType =>
                  val v = rs.getBigDecimal(colIndex)
                  if (rs.wasNull()) scalar.fillValue
                  else BigDecimalValue(v)
              }
              datumBuilder += datum
          }
          rowBuilder += datumBuilder.result()
          datumBuilder.clear()
          n -= 1
        }

        // Build Samples, account for projections
        val domainCount = getProjectedDomainCount(uops)
        rowBuilder.result().map { row =>
          //Apply operations to model then count non-Index domain variables
          val (dd, rd) = row.splitAt(domainCount)
          Sample(dd, rd)
        }
      }

    def liftProcessGeneric(
      chunkSize: Int,
      create: ConnectionIO[PreparedStatement],
      prep: PreparedStatementIO[Unit],
      exec: PreparedStatementIO[ResultSet]
    ): Stream[ConnectionIO, Sample] = {
      def prepared(ps: PreparedStatement): Stream[ConnectionIO, PreparedStatement] =
        eval[ConnectionIO, PreparedStatement] {
          val fs = FPS.setFetchSize(chunkSize)
          FC.embed(ps, fs *> prep).map(_ => ps)
        }

      def unrolled(rs: ResultSet): Stream[ConnectionIO, Sample] =
        repeatEvalChunks(FC.embed(rs, getNextChunkSamples(chunkSize)))

      val preparedStatement: Stream[ConnectionIO, PreparedStatement] =
        bracket(create)(FC.embed(_, FPS.close)).flatMap(prepared)

      def results(ps: PreparedStatement): Stream[ConnectionIO, Sample] =
        bracket(FC.embed(ps, exec))(FC.embed(_, FRS.close)).flatMap(unrolled)

      preparedStatement.flatMap(results)
    }

    def processGeneric(
      sql: String,
      prep: PreparedStatementIO[Unit],
      chunkSize: Int
    ): Stream[ConnectionIO, Sample] =
      liftProcessGeneric(chunkSize, FC.prepareStatement(sql), prep, FPS.executeQuery)

    val xa = Transactor.fromDriverManager[IO](
      config.driver,
      baseUri.toString,
      config.user,
      config.password,
      StreamUtils.blocker
    )

    val sql = SqlBuilder.buildQuery(config.table, model, uops, config.predicate)

    // processGeneric and the functions it calls were adapted from Doobie's
    // example.GenericStream to return Samples
    val result = processGeneric(sql, ().pure[PreparedStatementIO], fetchSize)
      .transact(xa)

    SampledFunction(result)
  }

}

object JdbcAdapter extends AdapterFactory {

  /** Constructor used by the AdapterFactory. */
  def apply(model: DataType, config: AdapterConfig): JdbcAdapter =
    new JdbcAdapter(model, JdbcAdapter.Config(config.properties: _*))

  /**
   * Defines a JdbcAdapter specific configuration with type-safe accessors for
   * driver, user, table, password, and predicate.
   */
  case class Config(properties: (String, String)*) extends ConfigLike {
    val driver: String = get("driver")
      .getOrElse(throw LatisException("DatabaseAdapter requires a driver definition."))
    val user: String = get("user")
      .getOrElse(throw LatisException("DatabaseAdapter requires a user definition."))
    val table: String = get("table")
      .getOrElse(throw LatisException("DatabaseAdapter requires a table definition."))
    val password: String = get("password")
      .getOrElse(throw LatisException("DatabaseAdapter requires a password definition."))
    val predicate: Option[String] = get("predicate")
  }

}

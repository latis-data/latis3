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
import latis.model._
import latis.ops._
import latis.time.Time
import latis.util.ConfigLike
import latis.util.LatisException
import latis.util.StreamUtils
import latis.util.StreamUtils.contextShift
import latis.util.dap2.parser.ast
import latis.util.StringUtil

/**
 * Defines an adapter for JDBC-supported databases.
 */
case class JdbcAdapter(
  model: DataType,
  config: JdbcAdapter.Config
) extends Adapter {
  //TODO: consider using https://tpolecat.github.io/doobie/docs/08-Fragments.html to build query
  //TODO: ensure that there are no nested functions, doesn't make sense for database table
  //TODO: consider lifecycle of database connections, when are they released
  //TODO: Support timestamp (and other non-numeric time) database column type.
  //      The query builder can handle this but not the result parsing.
  //TODO: support null values in database, fill with missing value
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
    case _             => false
  }

  /**
   * Gets the projected domain and range Scalars.
   *
   * If the given set of Operations has any Projections, the last will be used
   * to project the new model. Since this delegates to the Projection operation,
   * the order of variables in the model will be preserved as opposed to using
   * the order of the variables in the projection operation.
   *
   * If no Projection operation is provided, this will include all scalar
   * variables excluding Index variables since they are not explicitly
   * represented in the data.
   */
  private def getProjectedScalars(ops: Seq[Operation]): (Seq[Scalar], Seq[Scalar]) =
    (ops.collect {
      case p: Projection => p
    }.lastOption match {
      case Some(proj) =>
        //Delegate to Projection operation.
        proj.applyToModel(model)
          .fold(throw _, identity)
      case None => model
    }) match {
      case Function(d, r) => (d.getScalars.filterNot(_.isInstanceOf[Index]), r.getScalars)
      case dt => (List(), dt.getScalars)
    }

  /** Combines the domain and range Scalars. */
  private def getAllProjectedScalars(ops: Seq[Operation]): Seq[Scalar] =
    getProjectedScalars(ops) match {
      case (ds, rs) => ds ++ rs
    }

  /** Count the number of projected domain variables. */
  private def getProjectedDomainCount(ops: Seq[Operation]): Int =
    getProjectedScalars(ops) match {
      case (ds, _) => ds.length
    }

  /**
   * Constructs an SQL query based on the given operations.
   */
  private[input] def buildQuery(ops: Seq[Operation]): String = {
    // Collect the Rename operations and make Map of old name to new name
    val renameMap = ops.collect {
      case Rename(origId, newId) => (origId.asString, newId.asString)
    }.toMap

    // Construct the select clause
    val select = getAllProjectedScalars(ops)
      .map(_.id.get.asString)
      .map { name =>
        //use "as" to apply rename
        renameMap.get(name).fold(name)(nn => s"$name AS $nn")
      }
      .mkString("SELECT ", ", ", s" FROM ${config.table}")

    // Construct the where clause.
    val where = {
      val selections = ops.toList.collect {
        case Selection(id, sop, v) =>
          val value = model.findVariable(id) match {
            case Some(t: Time) =>
              // Interpret a time value as ISO or native numeric units.
              // If the database time column is non-numeric, this expects the time units
              // to reflect a format that works with the database.
              t.convertValue(v) match {
                case Right(Text(s)) => StringUtil.ensureSingleQuoted(s)
                case Right(n)       => n.asString
                case Left(le)       => throw LatisException(s"Invalid time value: $v", le)
              }
            case Some(s: Scalar) if (s.valueType == StringValueType) =>
              //Ensure that text variable value is quoted
              StringUtil.ensureSingleQuoted(v)
            case Some(_) => v
            case None =>
              //Should not get this far with an invalid id
              throw LatisException(s"Variable not found ${id.asString}")
          }
          val operator = sop match {
            //Note that unsupported operators are excluded in canHandleOperation
            //TODO: support EqTilde, NeEqTilde with "like"
            case ast.EqEq => "="
            case _        => ast.prettyOp(sop)
          }
          s"${id.asString} $operator $value"
      }
      //Prepend predicate if defined in config
      config.predicate.fold(selections)(p => p +: selections) match {
        case Nil => ""
        case ss  => ss.mkString(" WHERE ", " AND ", "")
      }
    }

    // Construct the order clause.
    // Order to be consistent with the model domain.
    // We need to order by all domain variables regardless of projection.
    // Note that the database can order by non-projected variables and use
    // their original names so no need to worry about rename.
    //TODO: support descending
    val order = {
      val nonIndexDomainVars = model match {
        case Function(domain, _) =>
          domain.getScalars
            .filterNot(_.isInstanceOf[Index])
            .map(_.id.get.asString)
        case _ => List.empty
      }
      if (nonIndexDomainVars.isEmpty) ""
      else nonIndexDomainVars.mkString(" ORDER BY ", ", ", " ASC")
    }

    // Construct the SQL query
    select + where + order
  }

  /**
   * Gets the max row count based on the given operations.
   * This supports only Operations allowed by canHandleOperation.
   */
  private[input] def getLimit(ops: Seq[Operation]): Option[Int] =
    ops.foldRight(Option.empty[Int]) {
      case (_, Some(0))         => Some(0)
      case (_: Head, _)         => Some(1)
      case (Take(n), Some(lim)) => Some(Math.min(n, lim))
      case (Take(n), None)      => Some(n)
      case (_, olim)            => olim
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

    // This escapes to raw JDBC for efficiency.
    def getNextChunkSamples(chunkSize: Int): ResultSetIO[Seq[Sample]] =
      FRS.raw { rs =>
        val md           = rs.getMetaData
        val ks           = (1 to md.getColumnCount).map(md.getColumnLabel).toList
        var n            = chunkSize
        val rowBuilder   = Vector.newBuilder[Seq[Datum]]
        val datumBuilder = Vector.newBuilder[Datum]
        while (n > 0 && rs.next) {
          ks.zip(getAllProjectedScalars(ops)).foreach { case (k, s) =>
            datumBuilder += Data.fromValue(s.valueType match {
              //TODO: deal with java.sql.Types.TIMESTAMP
              //      rs.getTimestamp(name, gmtCalendar).getTime will be native ms
              //TODO: deals with null, in latis2 we use rs.wasNull then make fill value
              case BooleanValueType => rs.getBoolean(k)
              case ByteValueType => rs.getByte(k)
              case CharValueType => rs.getByte(k).toChar
              case ShortValueType => rs.getShort(k)
              case IntValueType => rs.getInt(k)
              case LongValueType => rs.getLong(k)
              case FloatValueType => rs.getFloat(k)
              case DoubleValueType => rs.getDouble(k)
              case BinaryValueType => rs.getBytes(k)
              case StringValueType => rs.getString(k)
              case BigDecimalValueType => BigDecimal(rs.getBigDecimal(k))  // convert java BigDecimal to Scala BigDecimal
            }).fold(throw _, identity)
          }
          rowBuilder += datumBuilder.result()
          datumBuilder.clear()
          n -= 1
        }

        // Build Samples, account for projections
        val domainCount = getProjectedDomainCount(ops)
        rowBuilder.result().map { row =>
          //Apply operations to model then count non-Index domain variables
          val (dd, rd) = row.splitAt(domainCount)
          Sample(dd, rd)
        }
      }

    def liftProcessGeneric(
      limit: Option[Int],
      chunkSize: Int,
      create: ConnectionIO[PreparedStatement],
      prep: PreparedStatementIO[Unit],
      exec: PreparedStatementIO[ResultSet]
    ): Stream[ConnectionIO, Sample] = {
      def prepared(ps: PreparedStatement): Stream[ConnectionIO, PreparedStatement] =
        eval[ConnectionIO, PreparedStatement] {
          val fs = FPS.setFetchSize(chunkSize)
          val psio = limit match {
            case Some(lim) => fs *> FPS.setMaxRows(lim) *> prep
            case None      => fs *> prep
          }
          FC.embed(ps, psio).map(_ => ps)
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
      limit: Option[Int],
      chunkSize: Int
    ): Stream[ConnectionIO, Sample] =
      liftProcessGeneric(limit, chunkSize, FC.prepareStatement(sql), prep, FPS.executeQuery)

    val xa = Transactor.fromDriverManager[IO](
      config.driver,
      baseUri.toString,
      config.user,
      config.password,
      StreamUtils.blocker
    )

    val sql = buildQuery(ops)

    val limit = getLimit(ops)

    // processGeneric and the functions it calls were adapted from Doobie's
    // example.GenericStream to return Samples
    val result = processGeneric(sql, ().pure[PreparedStatementIO], limit, fetchSize)
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

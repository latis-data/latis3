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

import latis.data.Data
import latis.data.Datum
import latis.data.Sample
import latis.data.SampledFunction
import latis.model._
import latis.ops.Operation
import latis.util.ConfigLike
import latis.util.LatisException
import latis.util.StreamUtils
import latis.util.StreamUtils.contextShift

/**
 * Defines an adapter for JDBC-supported databases.
 */
case class JdbcAdapter(
  model: DataType,
  config: JdbcAdapter.Config
) extends Adapter {
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
          ks.zip(model.getScalars).foreach {
            case (k, s) =>
              datumBuilder += Data
                .fromValue(s.valueType match {
                  case BooleanValueType => rs.getBoolean(k)
                  case ByteValueType    => rs.getByte(k)
                  case CharValueType    => rs.getByte(k).toChar
                  case ShortValueType   => rs.getShort(k)
                  case IntValueType     => rs.getInt(k)
                  case LongValueType    => rs.getLong(k)
                  case FloatValueType   => rs.getFloat(k)
                  case DoubleValueType  => rs.getDouble(k)
                  case BinaryValueType  => rs.getBytes(k)
                  case StringValueType  => rs.getString(k)
                  //case BigIntValueType => rs.getObject(k) // there is no getBigInt method on result set
                  case BigDecimalValueType =>
                    BigDecimal(rs.getBigDecimal(k)) // convert java BigDecimal to Scala BigDecimal
                })
                .fold(throw _, identity)
          }
          rowBuilder += datumBuilder.result()
          datumBuilder.clear()
          n -= 1
        }
        for (row <- rowBuilder.result()) yield {
          val (dd, rd) = row.splitAt(model.arity)
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

    // List of column names. Looks for "sourceId" first, then "Id".
    val colNames: List[String] = model.getScalars.flatMap { s =>
      s("sourceId").orElse(s.id.map(_.asString))
    }
    // select columns from table
    val base = s"select ${colNames.mkString(", ")} from ${config.table} "
    // where clause if predicate is defined in the config
    val where = config.predicate.map(p => s"WHERE $p ").getOrElse("")
    // order by domain variables ascending
    val order = s"order by ${colNames.take(model.arity).mkString(", ")} asc "
    val sql   = base ++ where ++ order

    // processGeneric and the functions it calls were adapted from Doobie's
    // example.GenericStream to return Samples
    val result = processGeneric(sql, ().pure[PreparedStatementIO], 100)
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

package latis.input

import java.net.URI
import java.sql.ResultSetMetaData
import java.sql.ResultSet
import scala.io.Source

import cats.effect.Blocker
import cats.effect.IO
import cats.implicits._
import cats.syntax.all._
import doobie._
//import doobie.imports._
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.hi.resultset
import shapeless.record.Record
import shapeless.{::, HList, HNil}

import latis.data.Data
import latis.data.Datum
import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.model.DataType
import latis.ops.Operation
import latis.util.ConfigLike
import latis.util.LatisException

class DatabaseAdapter(
  model: DataType,
  config: DatabaseAdapter.Config
) extends Adapter {

  // We need a ContextShift[IO] before we can construct a Transactor[IO]. The passed ExecutionContext
  // is where nonblocking operations will be executed. For testing here we're using a synchronous EC.
  implicit val cs = IO.contextShift(ExecutionContexts.synchronous)

  def getData(baseUri: URI, ops: Seq[Operation]): Data = {
    // A transactor that gets connections from java.sql.DriverManager and executes blocking operations
    // on an our synchronous EC. See the chapter on connection handling for more info.
    val xa = Transactor.fromDriverManager[IO](
      "oracle.jdbc.OracleDriver",   // driver classname
      baseUri.toString,             // connect URL (driver-specific)
      config.user,                  // user
      config.password,              // password
      Blocker.liftExecutionContext(ExecutionContexts.synchronous) // just for testing
    )

//    val cols = model.getScalars.map(_.id).mkString(", ")
//    val selectFr = fr"select $cols from" ++ Fragment.const(config.table)
//    val whereFr = config.predicate.map {p => fr"where $p"}
//        .getOrElse(fr"")
//
//    val program = (selectFr ++ whereFr).query[(String)].to[List]

//    implicit val datumReader: Read[Datum] =
//      Read[Any].map { case x => Data.fromValue(x).getOrElse(throw LatisException(s"Failed to convert $x to Datum")) }
//    implicit val datumGetter: Get[Datum] =
//      Get[String].map { case x => Data.StringValue(x) }
//    implicit val datumReader: Read[Seq[Datum]] =
//      Read[String].map { case x => Data.StringValue(x) }
//    implicit val pointWrite: Write[HList] =
//      Write[(String, String)].contramap(p => (p(0).asInstanceOf[String], p(1).asInstanceOf[String]))
    type Row = Map[String, Any]
    case class StringyResult(header: List[String], rows: List[List[AnyRef]])
    def header(md: ResultSetMetaData): List[String] =
      (1 |-> md.getColumnCount).map(md.getColumnLabel)
    def row(md: ResultSetMetaData): ResultSetIO[List[AnyRef]] =
      (1 |-> md.getColumnCount).traverseU(FRS.getObject)
    def exec(sql: String): ConnectionIO[StringyResult] =
      HC.prepareStatement(sql)(HPS.executeQuery {
        for {
          md <- FRS.getMetaData
          rs <- row(md).whileM[List](HRS.next)
        } yield StringyResult(header(md), rs)
      })
    val result = sql"select timeGps, type from CPRS_MISC.OASISEVENTFILES FETCH FIRST 5 ROWS ONLY"
//      .query[ResultSetIO[Seq[Row]]]
      .query[ResultSetIO[(String, String)]]
//      .to[List]
      .stream
      .transact(xa)
      .compile
      .toList
      .unsafeRunSync()


    val samples = result.map { s =>
      Sample(
        DomainData(s.getString("timeGps")),
        RangeData(s.getString("type"), "Recording messages", "f19_dec_11_22_58_20.event_messages")
//        DomainData(model.getScalars.head.parseValue(s._1).getOrElse(throw LatisException("oops"))),
//        RangeData(s._2, s._3)
      )
    }
    SampledFunction(samples)
  }

}

object DatabaseAdapter extends AdapterFactory {

  def apply(model: DataType, config: AdapterConfig): DatabaseAdapter =
    new DatabaseAdapter(model, DatabaseAdapter.Config(config.properties: _*))

  case class Config(properties: (String, String)*) extends ConfigLike {
    val driver = "oracle.jdbc.OracleDriver"
    val user = "CPRS_LATIS_QUERY"
    val bufferedSource = Source.fromFile("/Users/ryhe6408/temp/.cprs_pw")
    // Of course we would find a better way to get passwords
    val password = bufferedSource.getLines.toList.head
    bufferedSource.close()

    val table: String = get("table")
      .getOrElse(throw LatisException("DatabaseAdapter requires a table definition."))
    val predicate: Option[String] = get("predicate")
  }

}

package latis.input

import java.net.URI

import latis.model.DataType
import latis.util.ConfigLike
import scala.io.Source

import cats.effect.Blocker
import doobie.util.ExecutionContexts
import cats.effect.IO
import cats.syntax.all._
import doobie._
import doobie.implicits._
import oracle.jdbc.OracleDriver
import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.Data
import latis.data.SampledFunction
import latis.ops.Operation
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
      "oracle.jdbc.OracleDriver",     // driver classname
      baseUri.toString,     // connect URL (driver-specific)
      config.user,                  // user
      config.password,                          // password
      Blocker.liftExecutionContext(ExecutionContexts.synchronous) // just for testing
    )

    val cols = model.getScalars.map(_.id).mkString(", ")
    val selectFr = fr"select $cols from" ++ Fragment.const(config.table)
    val whereFr = config.predicate.map {p => fr"where $p"}
        .getOrElse(fr"")

    val program = (selectFr ++ whereFr).query[(String)].to[List]
    val samples = program.transact(xa).unsafeRunSync()
      .map { s =>
      Sample(
        DomainData(s),
        RangeData("foo")
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
    val bufferedSource = Source.fromFile("Users/ryhe6408/temp/.cprs_pw")
    // Of course we would find a better way to get passwords
    val password = bufferedSource.getLines.toList.head
    bufferedSource.close()

    val table: String = get("table")
      .getOrElse(throw LatisException("DatabaseAdapter requires a table definition."))
    val predicate: Option[String] = get("predicate")
  }

}

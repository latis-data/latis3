package latis.input

import java.io.File
import java.net.URI

import cats.effect.ContextShift
import cats.effect.IO
import cats.syntax.all._
import doobie._
import doobie.implicits._
import org.scalatest.Inside._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.data.Data.DoubleValue
import latis.model.ModelParser.parse
import latis.ops.Projection
import latis.ops.Selection

class JdbcAdapterSpec extends AnyFlatSpec {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContexts.synchronous)

  "The JdbcAdapter" should "read data from a database table" in
    withDatabase { uri =>
      val model: latis.model.DataType =
        parse("(time: Int, wavelength: Double) -> (flux: Double, error: Double)")
          .fold(throw _, identity)
      val adapter = new JdbcAdapter(
        model,
        JdbcAdapter.Config(
          ("driver", "org.h2.Driver"),
          ("user", ""),
          ("table", "tab1"),
          ("password", "")
        )
      )
      lazy val actualFirstSample = adapter.getData(uri).samples.take(1).compile.last.unsafeRunSync().getOrElse {
        fail("Empty Dataset")
      }
      inside(actualFirstSample) {
        case (dd, rd) =>
          assert(dd == DomainData(Data.IntValue(1), 1.1))
          assert(rd == RangeData(2.1, 0.1))
      }
    }

  it should "read data from joined database tables" in
    withDatabase { uri =>
      val model: latis.model.DataType =
        parse("time: Int -> message: String")
          .fold(throw _, identity)
      val adapter = new JdbcAdapter(
        model,
        JdbcAdapter.Config(
          ("driver", "org.h2.Driver"),
          ("user", ""),
          ("table", "tab1, tab2"),
          ("predicate", "tab1.time = tab2.seconds"),
          ("password", "")
        )
      )

      val ops = List(
        Selection.makeSelection("message = bar").toTry.get,
        Projection.fromExpression("message").toTry.get
      )
      adapter.getData(uri, ops).samples.compile.toList.unsafeRunSync() //.foreach(println)
        .head match {
          case Sample(DomainData(), RangeData(Text(msg))) =>
            assert(msg == "bar")
        }
    }

  it should "capture exceptions in the stream" in
    withDatabase { uri =>
      val model: latis.model.DataType =
        parse("seconds: Int -> message: Double")
          .fold(throw _, identity)
      val adapter = new JdbcAdapter(
        model,
        JdbcAdapter.Config(
          ("driver", "org.h2.Driver"),
          ("user", ""),
          ("table", "tab2"),
          ("password", "")
        )
      )
      adapter.getData(uri).samples.compile.toList.attempt.unsafeRunSync() match {
        case Left(_) => succeed
        case _ => fail()
      }
    }

  private def withDatabase(f: URI => Any): Any =
    Option(File.createTempFile(this.suiteName, ".mv.db")) match {
      case Some(file) =>
        file.deleteOnExit()
        val connectionUrl = s"jdbc:h2:${file.getAbsolutePath}"
        createMockDatabase(connectionUrl)
        f(new URI(connectionUrl))
      case None => cancel("Could not create temporary database file for testing.")
    }

  private def createMockDatabase(url: String): Unit = {
    val xa = Transactor.fromDriverManager[IO]("org.h2.Driver", url)

    val drop =
      sql"""
       DROP TABLE IF EXISTS tab1;
       DROP TABLE IF EXISTS tab2;
     """.update.run
    val create =
      sql"""
       CREATE TABLE tab1(
         time INT,
         wavelength DOUBLE,
         flux  DOUBLE,
         error DOUBLE
       );
       CREATE TABLE tab2(
         seconds INT,
         message CLOB
       );
     """.update.run
    val insert =
      sql"""
       INSERT INTO tab1 VALUES (1, 1.1, 2.1, 0.1);
       INSERT INTO tab1 VALUES (2, 1.2, 2.2, 0.2);
       INSERT INTO tab2 VALUES (1, 'foo');
       INSERT INTO tab2 VALUES (1, 'bar');
     """.update.run
    (drop, create, insert).mapN(_ + _ + _).transact(xa).unsafeRunSync
  }
}

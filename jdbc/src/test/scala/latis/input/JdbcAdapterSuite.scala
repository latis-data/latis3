package latis.input

import java.net.URI

import cats.effect.IO
import cats.syntax.all._
import doobie._
import doobie.implicits._
import fs2.io.file.Files
import munit.CatsEffectSuite

import latis.data._
import latis.dsl.ModelParser.parse
import latis.model.DataType
import latis.ops._
import latis.util.Identifier._

class JdbcAdapterSuite extends CatsEffectSuite {

  private val database = ResourceFixture {
    Files[IO]
      .tempFile(None, "JdbcAdapterSuite", ".mv.db", None)
      .evalMap { path =>
        val connectionUrl = s"jdbc:h2:${path.absolute.toString}"
        createMockDatabase(connectionUrl).as(new URI(connectionUrl))
      }
  }

  database.test("read data from a database table") { uri =>
    val model: DataType =
      parse("(time: Int, wavelength: Double) -> (flux: Double, error: Double)")
        .fold(fail("Failed to construct model", _), identity)

    val adapter = new JdbcAdapter(
      model,
      JdbcAdapter.Config(
        ("driver", "org.h2.Driver"),
        ("user", ""),
        ("table", "tab1"),
        ("password", "")
      )
    )

    val ops = List(
      Take(1) //apparently "FETCH FIRST n ROWS ONLY" works for h2
    )

    adapter.getData(uri, ops).samples.compile.toList.map {
      case Sample(DomainData(Integer(t), Real(w)), RangeData(Real(f), Real(e))) :: Nil =>
        assertEquals(t, 1L)
        assertEquals(w, 1.1)
        assertEquals(f, 2.1)
        assertEquals(e, 0.1)
      case _ :: Nil => fail("wrong sample")
      case _ :: _ => fail("too many samples")
      case Nil => fail("no samples")
    }
  }

  database.test("read data from joined database tables") { uri =>
    val model: DataType =
      parse("time: Int -> message: String")
        .fold(fail("Failed to construct model", _), identity)

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
      Rename(id"message", id"messagez"),
      Selection.makeSelection("messagez = bar").toTry.get,
      Projection.fromExpression("messagez").toTry.get
    )

    adapter.getData(uri, ops).samples.compile.toList.map { ss =>
      ss.head match {
        case Sample(DomainData(), RangeData(Text(msg))) =>
          assert(msg == "bar")
        case _ => fail("wrong sample")
      }
    }
  }

  database.test("capture exceptions in the stream") { uri =>
    val model: DataType =
      parse("seconds: Int -> message: Double")
        .fold(fail("failed to construct model", _), identity)

    val adapter = new JdbcAdapter(
      model,
      JdbcAdapter.Config(
        ("driver", "org.h2.Driver"),
        ("user", ""),
        ("table", "tab2"),
        ("password", "")
      )
    )

    adapter.getData(uri).samples.compile.drain.intercept[Throwable]
  }

  database.test("count data from a database table") { uri =>
    val model: DataType =
      parse("(time: Int, wavelength: Double) -> (flux: Double, error: Double)")
        .fold(fail("Failed to construct model", _), identity)

    val adapter = new JdbcAdapter(
      model,
      JdbcAdapter.Config(
        ("driver", "org.h2.Driver"),
        ("user", ""),
        ("table", "tab1"),
        ("password", "")
      )
    )

    val ops = List(
      CountAggregation()
    )

    adapter.getData(uri, ops).samples.compile.toList.map {
      case Sample(DomainData(), RangeData(Integer(c))) :: Nil =>
        assertEquals(c, 2L)
      case _ :: Nil => fail("wrong sample")
      case _ :: _   => fail("too many samples")
      case Nil      => fail("no samples")
    }
  }

  //=== Mock database ===//

  private def createMockDatabase(url: String): IO[Unit] = {
    val xa = Transactor.fromDriverManager[IO]("org.h2.Driver", url, None)

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

    (drop, create, insert).tupled.void.transact(xa)
  }
}

package latis.service.sql

import cats.effect.IO
import org.http4s.EntityEncoder
import org.http4s.Method
import org.http4s.Request
import org.http4s.Status
import org.http4s.implicits.*

import latis.catalog.Catalog
import latis.dsl.DatasetGenerator
import latis.util.Identifier.*

class SqlServiceSuite extends munit.CatsEffectSuite {

  private val service = {
    val catalog = {
      val ds = DatasetGenerator("x -> y", id"ds")
      Catalog(ds)
    }

    SqlService(catalog).routes.orNotFound
  }

  test("Return 200 after successful query") {
    val query = "select * from ds"
    val request = Request[IO](
      Method.POST,
      uri"/",
      body = EntityEncoder[IO, String].toEntity(query).body
    )

    service(request).map { response =>
      assertEquals(response.status, Status.Ok, "non-200 status")
    }
  }

  test("Return 400 after failing to find dataset") {
    val query = "select * from missing"
    val request = Request[IO](
      Method.POST,
      uri"/",
      body = EntityEncoder[IO, String].toEntity(query).body
    )

    service(request).map { response =>
      assertEquals(response.status, Status.BadRequest, "non-400 status")
    }
  }

  test("Return 400 after failing to parse query") {
    val query = "not even sql"
    val request = Request[IO](
      Method.POST,
      uri"/",
      body = EntityEncoder[IO, String].toEntity(query).body
    )

    service(request).map { response =>
      assertEquals(response.status, Status.BadRequest, "non-400 status")
    }
  }
}

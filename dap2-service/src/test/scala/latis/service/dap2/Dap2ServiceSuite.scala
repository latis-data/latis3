package latis.service.dap2

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.http4s._
import org.http4s.implicits.http4sLiteralsSyntax
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside
import org.typelevel.ci.CIStringSyntax

import latis.catalog.Catalog
import latis.dsl.DatasetGenerator
import latis.util.Identifier.IdentifierStringContext

class Dap2ServiceSuite extends AnyFunSuite {
  //TODO: test other error handling
  //  bad query
  //  error after streaming

  private lazy val ds0 = DatasetGenerator("x -> a", id"ds0")
  private lazy val ds1 = DatasetGenerator("y -> b", id"ds1")

  private lazy val catalog: Catalog =
    Catalog(ds0).addCatalog(id"cat1", Catalog(ds1))

  private lazy val dap2Service = new Dap2Service(catalog)

  def showResponse(uri: Uri): Unit = {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri)).flatMap { response =>
      IO.println(response.status) >>
        response.bodyText.evalMap(IO.println).compile.drain
    }.unsafeRunSync()
  }

  test("top level catalog") {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/")).map { response =>
      assert(response.status == Status.Ok)
    }.unsafeRunSync()
  }

  test("resource not found") {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/foo")).map { response =>
      assert(response.status == Status.NotFound)
    }.unsafeRunSync()
  }

  test("invalid id") {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/foo.bar/baz")).map { response =>
      assert(response.status == Status.NotFound)
    }.unsafeRunSync()
  }

  test("nested catalog independent of trailing slash") {
    (for {
      resp1 <- dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/cat1"))
      resp2 <- dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/cat1/"))
      body1 <- resp1.bodyText.compile.toList.map(_.mkString)
      body2 <- resp2.bodyText.compile.toList.map(_.mkString)
    } yield {
      assert(resp1.status == Status.Ok)
      assert(body1 == body2)
    }).unsafeRunSync()
  }

  test("dataset with extension") {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/ds0.meta")).map { response =>
      assert(response.status == Status.Ok)
      inside(response.headers.get(ci"Content-Type")) {
        case Some(NonEmptyList(header, _)) => assertResult("application/json")(header.value)
      }
    }.unsafeRunSync()
  }

  test("dataset without extension") {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/ds0")).map { response =>
      assert(response.status == Status.Ok)
      inside(response.headers.get(ci"Content-Type")) {
        case Some(NonEmptyList(header, _)) => assertResult("application/json")(header.value)
      }
    }.unsafeRunSync()
  }

  test("nested dataset with extension") {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/cat1/ds1.meta")).map { response =>
      assert(response.status == Status.Ok)
      inside(response.headers.get(ci"Content-Type")) {
        case Some(NonEmptyList(header, _)) => assertResult("application/json")(header.value)
      }
    }.unsafeRunSync()
  }

  test("nested dataset without extension") {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/cat1/ds1")).map { response =>
      assert(response.status == Status.Ok)
      inside(response.headers.get(ci"Content-Type")) {
        case Some(NonEmptyList(header, _)) => assertResult("application/json")(header.value)
      }
    }.unsafeRunSync()
  }

  test("dataset not found with trailing slash") {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/cat1/ds1/")).map { response =>
      assert(response.status == Status.NotFound)
    }.unsafeRunSync()
  }

  test("dataset not found with extension and trailing slash") {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/cat1/ds1.meta/")).map { response =>
      assert(response.status == Status.NotFound)
    }.unsafeRunSync()
  }

  test("dataset not found with trailing dot") {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/cat1/ds1.")).map { response =>
      assert(response.status == Status.NotFound)
    }.unsafeRunSync()
  }

  test("dataset not found with extension and trailing dot") {
    dap2Service.routes.orNotFound(Request[IO](Method.GET, uri"/cat1/ds1.meta.")).map { response =>
      assert(response.status == Status.NotFound)
    }.unsafeRunSync()
  }

  //---- Test catalog content negotiation ----//

  test("negotiate json response") {
    val headers = Headers(Header.Raw(ci"Accept", "application/json,text/html"))
    val req = Request[IO](Method.GET, uri"/", headers=headers)
    dap2Service.routes.orNotFound(req).map { resp =>
      assertResult("application/json")(resp.headers.get(ci"Content-Type").get.head.value)
    }.unsafeRunSync()
  }

  test("negotiate json response by default") {
    val headers = Headers()
    val req = Request[IO](Method.GET, uri"/", headers=headers)
    dap2Service.routes.orNotFound(req).map { resp =>
      assertResult("application/json")(resp.headers.get(ci"Content-Type").get.head.value)
    }.unsafeRunSync()
  }

  test("negotiate json response over all") {
    val headers = Headers(Header.Raw(ci"Accept", "*/*"))
    val req = Request[IO](Method.GET, uri"/", headers=headers)
    dap2Service.routes.orNotFound(req).map { resp =>
      assertResult("application/json")(resp.headers.get(ci"Content-Type").get.head.value)
    }.unsafeRunSync()
  }

  test("negotiate json response over image") {
    val headers = Headers(Header.Raw(ci"Accept", "image/*,application/json"))
    val req = Request[IO](Method.GET, uri"/", headers=headers)
    dap2Service.routes.orNotFound(req).map { resp =>
      assertResult("application/json")(resp.headers.get(ci"Content-Type").get.head.value)
    }.unsafeRunSync()
  }

  test("negotiate html response") {
    val headers = Headers(Header.Raw(ci"Accept", "text/*,application/json"))
    val req = Request[IO](Method.GET, uri"/", headers=headers)
    dap2Service.routes.orNotFound(req).map { resp =>
      assert(resp.headers.get(ci"Content-Type").get.head.value.startsWith("text/html"))
    }.unsafeRunSync()
  }
}

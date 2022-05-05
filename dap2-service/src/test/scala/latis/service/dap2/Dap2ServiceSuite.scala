package latis.service.dap2

import cats.effect.IO
import cats.syntax.all._
import munit.CatsEffectSuite
import org.http4s._
import org.http4s.headers.`Content-Length`
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.typelevel.ci._

import latis.catalog.Catalog
import latis.dataset.MemoizedDataset
import latis.dsl.DatasetGenerator
import latis.metadata.Metadata
import latis.util.Identifier._

class Dap2ServiceSuite extends CatsEffectSuite {
  //TODO: test other error handling
  //  bad query
  //  error after streaming

  private val service = {
    val catalog = {
      val ds0 = DatasetGenerator("x -> a", id"ds0")
      val ds1 = DatasetGenerator("y -> b", id"ds1")
      Catalog(ds0).addCatalog(id"cat1", Catalog(ds1))
    }
    new Dap2Service(catalog).routes.orNotFound
  }

  test("create a non-zero length '200 OK' response") {
    val req = Request[IO](
      Method.GET,
      uri"/",
      headers=Headers(Header.Raw(ci"Host", "testhost:0000"))
    )

    service(req).map { res =>
      assertEquals(res.status, Status.Ok, "non-200 status")

      res.headers.get[`Content-Length`].map(_.length) match {
        case Some(length) => assert(length > 0, "zero length response")
        case None => fail("no content-length header")
      }
    }
  }

  test("correctly generate the catalog table") {
    val dataset1 = new MemoizedDataset(
      Metadata("id"->"id1", "title"->"title1"),
      null,
      null
    )

    val dataset2 = new MemoizedDataset(
      Metadata("id"->"id2", "title"->"title2"),
      null,
      null
    )

    val catalog = Catalog(dataset1, dataset2)

    HtmlCatalogEncoder.catalogTable(catalog).map { c =>
      val expected =
        """<table>
          |<caption><b><i><u>Catalog</u></i></b></caption>
          |<tr>
          |<th>id</th>
          |<th>title</th>
          |</tr>
          |<tr>
          |<td>id1</td>
          |<td><a href="id1.meta">title1</a></td>
          |</tr>
          |<tr>
          |<td>id2</td>
          |<td><a href="id2.meta">title2</a></td>
          |</tr>
          |</table>""".stripMargin.replaceAll(System.lineSeparator(), "")
      assertEquals(c.render, expected)
    }
  }

  test("top level catalog") {
    service(Request[IO](Method.GET, uri"/")).map { response =>
      assertEquals(response.status, Status.Ok)
    }
  }

  test("resource not found") {
    service(Request[IO](Method.GET, uri"/foo")).map { response =>
      assertEquals(response.status, Status.NotFound)
    }
  }

  test("invalid id") {
    service(Request[IO](Method.GET, uri"/foo.bar/baz")).map { response =>
      assertEquals(response.status, Status.NotFound)
    }
  }

  test("nested catalog independent of trailing slash") {
    (
      service(Request[IO](Method.GET, uri"/cat1")),
      service(Request[IO](Method.GET, uri"/cat1/"))
    ).tupled.flatMap { case (resA, resB) =>
        assertEquals(resA.status, Status.Ok)

        (
          resA.bodyText.compile.string,
          resB.bodyText.compile.string
        ).mapN { case (a, b) => assertEquals(a, b) }
    }
  }

  test("dataset with extension") {
    service(Request[IO](Method.GET, uri"/ds0.meta")).map { response =>
      assertEquals(response.status, Status.Ok)

      response.headers.get[`Content-Type`] match {
        case Some(ct) => assertEquals(ct.mediaType, MediaType.application.json)
        case None => fail("missing content-type header")
      }
    }
  }

  test("dataset without extension") {
    service(Request[IO](Method.GET, uri"/ds0")).map { response =>
      assertEquals(response.status, Status.Ok)

      response.headers.get[`Content-Type`] match {
        case Some(ct) => assertEquals(ct.mediaType, MediaType.application.json)
        case None => fail("missing content-type header")
      }
    }
  }

  test("nested dataset with extension") {
    service(Request[IO](Method.GET, uri"/cat1/ds1.meta")).map { response =>
      assertEquals(response.status, Status.Ok)

      response.headers.get[`Content-Type`] match {
        case Some(ct) => assertEquals(ct.mediaType, MediaType.application.json)
        case None => fail("missing content-type header")
      }
    }
  }

  test("nested dataset without extension") {
    service(Request[IO](Method.GET, uri"/cat1/ds1")).map { response =>
      assertEquals(response.status, Status.Ok)

      response.headers.get[`Content-Type`] match {
        case Some(ct) => assertEquals(ct.mediaType, MediaType.application.json)
        case None => fail("missing content-type header")
      }
    }
  }

  test("dataset not found with trailing slash") {
    service(Request[IO](Method.GET, uri"/cat1/ds1/")).map { response =>
      assertEquals(response.status, Status.NotFound)
    }
  }

  test("dataset not found with extension and trailing slash") {
    service(Request[IO](Method.GET, uri"/cat1/ds1.meta/")).map { response =>
      assertEquals(response.status, Status.NotFound)
    }
  }

  test("dataset not found with trailing dot") {
    service(Request[IO](Method.GET, uri"/cat1/ds1.")).map { response =>
      assertEquals(response.status, Status.NotFound)
    }
  }

  test("dataset not found with extension and trailing dot") {
    service(Request[IO](Method.GET, uri"/cat1/ds1.meta.")).map { response =>
      assertEquals(response.status, Status.NotFound)
    }
  }

  //---- Test catalog content negotiation ----//

  test("negotiate json response") {
    val headers = Headers(Header.Raw(ci"Accept", "application/json,text/html"))
    val req = Request[IO](Method.GET, uri"/", headers=headers)
    service(req).map { resp =>
      resp.headers.get[`Content-Type`] match {
        case Some(ct) => assertEquals(ct.mediaType, MediaType.application.json)
        case None => fail("missing content-type header")
      }
    }
  }

  test("negotiate json response by default") {
    val headers = Headers()
    val req = Request[IO](Method.GET, uri"/", headers=headers)
    service(req).map { resp =>
      resp.headers.get[`Content-Type`] match {
        case Some(ct) => assertEquals(ct.mediaType, MediaType.application.json)
        case None => fail("missing content-type header")
      }
    }
  }

  test("negotiate json response over all") {
    val headers = Headers(Header.Raw(ci"Accept", "*/*"))
    val req = Request[IO](Method.GET, uri"/", headers=headers)
    service(req).map { resp =>
      resp.headers.get[`Content-Type`] match {
        case Some(ct) => assertEquals(ct.mediaType, MediaType.application.json)
        case None => fail("missing content-type header")
      }
    }
  }

  test("negotiate json response over image") {
    val headers = Headers(Header.Raw(ci"Accept", "image/*,application/json"))
    val req = Request[IO](Method.GET, uri"/", headers=headers)
    service(req).map { resp =>
      resp.headers.get[`Content-Type`] match {
        case Some(ct) => assertEquals(ct.mediaType, MediaType.application.json)
        case None => fail("missing content-type header")
      }
    }
  }

  test("negotiate html response") {
    val headers = Headers(Header.Raw(ci"Accept", "text/*,application/json"))
    val req = Request[IO](Method.GET, uri"/", headers=headers)
    service(req).map { resp =>
      resp.headers.get[`Content-Type`] match {
        case Some(ct) => assertEquals(ct.mediaType, MediaType.text.html)
        case None => fail("missing content-type header")
      }
    }
  }
}

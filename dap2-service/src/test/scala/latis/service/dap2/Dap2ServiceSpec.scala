package latis.service.dap2

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.http4s
import org.http4s._
import org.http4s.implicits.http4sLiteralsSyntax
import org.scalatest.flatspec.AnyFlatSpec
import org.typelevel.ci.CIStringSyntax

import latis.catalog.Catalog
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata

class Dap2ServiceSpec extends AnyFlatSpec {

  private lazy val dataset1 = new MemoizedDataset(Metadata("id"->"id1", "title"->"title1"), null, null)
  private lazy val dataset2 = new MemoizedDataset(Metadata("id"->"id2", "title"->"title2"), null, null)
  private lazy val dap2Service = new Dap2Service(Catalog(dataset1, dataset2))

  "The Dap2 Landing Page" should "create a non-zero length '200 OK' response" in {
    val req = Request[IO](Method.GET, uri"/", headers=Headers(Header.Raw(ci"Host", "testhost:0000")))
    (for {
      response <- dap2Service.routes.orNotFound(req)
    } yield {
      assert(response.status == http4s.Status.Ok)
      assert(response.headers.get(ci"Content-Length").get.head.value.toInt > 0)
    }).unsafeRunSync()
  }

  it should "correctly generate the catalog table" in {
    (for {
      catalog <- dap2Service.catalogTable
    } yield {
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
      assert(catalog.render == expected)
    }).unsafeRunSync()
  }
}

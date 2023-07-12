package latis.service.landing

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.Method
import org.http4s.Request
import org.http4s.Response
import org.http4s.Status
import org.http4s.headers.`Content-Length`
import org.http4s.implicits.*

import latis.server.ServiceInfo

class LandingPageServiceSuite extends CatsEffectSuite {
  val serviceInfo: ServiceInfo = ServiceInfo(
    "Test Server",
    Some("0.0.1"),
    Some("3.0.0"),
    Some("10:35am 3/23/2022")
  )
  val landingPageService = new DefaultLandingPage(serviceInfo)
  val req: Request[IO] = Request[IO](Method.GET, uri"/")
  val resp: IO[Response[IO]] = landingPageService.routes.orNotFound(req)

  test("generate a non-zero length '200 OK' response") {
    resp.map { res =>
      assertEquals(res.status, Status.Ok)
      res.headers.get[`Content-Length`].map(_.length) match {
        case Some(length) => assert(length > 0, "zero length response")
        case None => fail("no content length header")
      }
    }
  }

  test("produce all of the ServiceInfo information") {
    resp.flatMap { res =>
      res.bodyText.compile.string
    }.map { body =>
      assert(body.contains(serviceInfo.service), "service not present in HTML")
      assert(body.contains(serviceInfo.version.get), "version not present in HTML")
      assert(body.contains(serviceInfo.latisVersion.get), "latisVersion not present in HTML")
      assert(body.contains(serviceInfo.buildTime.get), "buildTime not present in HTML")
    }
  }

}

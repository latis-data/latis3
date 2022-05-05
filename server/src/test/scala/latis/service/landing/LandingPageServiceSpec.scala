package latis.service.landing

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.http4s
import org.http4s.Method
import org.http4s.Request
import org.http4s.Response
import org.http4s.implicits.http4sLiteralsSyntax
import org.scalatest.flatspec.AnyFlatSpec
import org.typelevel.ci.CIStringSyntax

import latis.server.ServiceInfo

class LandingPageServiceSpec extends AnyFlatSpec {
  val serviceInfo: ServiceInfo = ServiceInfo("Test Server", Some("0.0.1"), Some("3.0.0"), Some("10:35am 3/23/2022"))
  val landingPageService = new LandingPageService(serviceInfo)
  val req: Request[IO] = Request[IO](Method.GET, uri"/")
  val resp: IO[Response[IO]] = landingPageService.routes.orNotFound(req)

  "The Landing Page Service" should "generate a non-zero length '200 OK' response" in {
    (for {
      response <- resp
    } yield {
      assert(response.status == http4s.Status.Ok)
      assert(response.headers.get(ci"Content-Length").get.head.value.toInt > 0)
    }).unsafeRunSync()
  }

  it should "contain all of the ServiceInfo information" in {
    (for {
      response <- resp
      body <- response.bodyText.compile.toList
    } yield {
      assert(body.head contains serviceInfo.service)
      assert(body.head contains serviceInfo.version.get)
      assert(body.head contains serviceInfo.latisVersion.get)
      assert(body.head contains serviceInfo.buildTime.get)
    }).unsafeRunSync()
  }

}

package latis.service.landing

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.http4s
import org.http4s.Method
import org.http4s.Request
import org.http4s.implicits.http4sLiteralsSyntax
import org.scalatest.flatspec.AnyFlatSpec
import org.typelevel.ci.CIStringSyntax

import latis.server.Latis3ServerBuilder
import latis.server.ServiceInfo

case object BuildInfo {
  val name = "Test Server"
  val version = "0.0.1"
  val latisVersion = "3.0.0"
  val buildTime = "10:35am 3/23/2022"
}

class LandingPageServiceSpec extends AnyFlatSpec{
  val serviceInfo: ServiceInfo = Latis3ServerBuilder.makeServiceInfo("latis.service.landing.BuildInfo$")
  val landingPageService = new LandingPageService(serviceInfo)

  "The Landing Page Service" should "generate the correct ServiceInfo from a provided object name" in {
    assert(serviceInfo.name == "Test Server")
    assert(serviceInfo.version.get == "0.0.1")
    assert(serviceInfo.latisVersion.get == "3.0.0")
    assert(serviceInfo.buildTime.get == "10:35am 3/23/2022")
  }

  it should "generate a non-zero length '200 OK' response" in {
    val req = Request[IO](Method.GET, uri"/")
    (for {
      response <- landingPageService.routes.orNotFound(req)
    } yield {
      assert(response.status == http4s.Status.Ok)
      assert(response.headers.get(ci"Content-Length").get.head.value.toInt > 0)
    }).unsafeRunSync()
  }

}

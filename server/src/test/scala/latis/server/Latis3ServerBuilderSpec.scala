package latis.server

import org.scalatest.flatspec.AnyFlatSpec

case object BuildInfo {
  val service = "Test Server"
  val version = "0.0.1"
  val latisVersion = "3.0.0"
  val buildTime = "10:35am 3/23/2022"
}

class Latis3ServerBuilderSpec extends AnyFlatSpec{
  val serviceInfo: ServiceInfo = Latis3ServerBuilder.makeServiceInfo("latis.server.BuildInfo$")

  "The Latis3 Server Builder" should "generate the correct ServiceInfo from a provided object name" in {
    assert(serviceInfo.service == "Test Server")
    assert(serviceInfo.version.get == "0.0.1")
    assert(serviceInfo.latisVersion.get == "3.0.0")
    assert(serviceInfo.buildTime.get == "10:35am 3/23/2022")
  }

}

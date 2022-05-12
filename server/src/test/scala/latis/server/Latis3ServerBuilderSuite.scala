package latis.server

import munit.CatsEffectSuite

case object BuildInfo {
  val service = "Test Server"
  val version = "0.0.1"
  val latisVersion = "3.0.0"
  val buildTime = "10:35am 3/23/2022"
}

class Latis3ServerBuilderSuite extends CatsEffectSuite {
  val serviceInfo: ServiceInfo = Latis3ServerBuilder.makeServiceInfo("latis.server.BuildInfo$")

  test("generate the correct ServiceInfo from a provided object name") {
    assertEquals(serviceInfo.service, "Test Server")
    assertEquals(serviceInfo.version.get, "0.0.1")
    assertEquals(serviceInfo.latisVersion.get, "3.0.0")
    assertEquals(serviceInfo.buildTime.get, "10:35am 3/23/2022")
  }

}

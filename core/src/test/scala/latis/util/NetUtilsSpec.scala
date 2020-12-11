package latis.util

import java.net.URI

import cats.syntax.all._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.util.NetUtils._

class NetUtilsSpec extends FlatSpec {

  private val uri1 = new URI("http://user:password@host:123/URI")
  private val uri2 = new URI("http://user:password@host:port/URI")
  private val uri3 = new URI("http://use%2Fr:p%40ss%3Aword@host:123/URI")

  "getHost" should "parse the host from a URI" in {
    getHost(uri1) should be ("host".asRight)
  }

  it should "return left if the host can't be parsed" in {
    getHost(uri2) shouldBe a [Left[LatisException, String]]
  }

  "getPort" should "parse the port from a URI" in {
    getPort(uri1) should be (123.asRight)
  }

  it should "return left if the port can't be parsed" in {
    getPort(uri2) shouldBe a [Left[LatisException, Int]]
  }

  "getPortOrDefault" should "return a default port if port can't be parsed" in {
    getPortOrDefault(uri2) should be (80.asRight)
  }

  "getUserInfo" should "parse user and password from a URI" in {
    getUserInfo(uri1) should be (("user", "password").asRight)
  }

  it should "parse user and password with encoded symbols from a URI" in {
    getUserInfo(uri3) should be (("use/r", "p@ss:word").asRight)
  }

  it should "return left if user info can't be parsed" in {
    getUserInfo(uri2) shouldBe a [Left[LatisException, (String, String)]]
  }
}

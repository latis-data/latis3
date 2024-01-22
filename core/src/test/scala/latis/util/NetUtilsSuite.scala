package latis.util

import java.net.URI

import munit.FunSuite

import latis.util.NetUtils.*

class NetUtilsSuite extends FunSuite {

  private lazy val uri1 = new URI("http://user:password@host:123/URI")
  private lazy val uri2 = new URI("http://user:password@host:port/URI")
  private lazy val uri3 = new URI("http://use%2Fr:p%40ss%3Aword@host:123/URI")

  test("getHost should parse the host from a URI") {
    assertEquals(getHost(uri1), Right("host"))
  }

  test("getHost should return Left if the host can't be parsed") {
    assert(getHost(uri2).isLeft, "was not Left")
  }

  test("getPort should parse the port from a URI") {
    assertEquals(getPort(uri1), Right(123))
  }

  test("getPort should return Left if the port can't be parsed") {
    assert(getPort(uri2).isLeft, "was not Left")
  }

  test("getPortOrDefault should return a default port if port can't be parsed") {
    assertEquals(getPortOrDefault(uri2), Right(80))
  }

  test("getUserInfo should parse user and password from a URI") {
    assertEquals(getUserInfo(uri1), Right(("user", "password")))
  }

  test("getUserInfo should parse user and password with encoded symbols from a URI") {
    assertEquals(getUserInfo(uri3), Right(("use/r", "p@ss:word")))
  }

  test("getUserInfo should return Left if user info can't be parsed") {
    assert(getUserInfo(uri2).isLeft, "was not Left")
  }
}

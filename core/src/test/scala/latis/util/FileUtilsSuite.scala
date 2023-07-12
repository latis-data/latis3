package latis.util

import java.net.URI
import java.nio.file.Paths

import munit.FunSuite

class FileUtilsSuite extends FunSuite {

  /*
   * This is designed to find the file in the local file system,
   * a case where tests have failed before.
   * Some testing environments may find it in a jar file.
   * If they do, then they didn't suffer from the original problem.
   */
  test("find a file in the classpath") {
    val path = Paths.get("fdml.xsd")
    assert(FileUtils.resolvePath(path).nonEmpty)
  }

  test("find a file in a jar in the classpath".ignore) {
    val path = Paths.get("munit/FunSuite.class")

    assertEquals(
      FileUtils.resolvePath(path).get.toString(),
      "/munit/FunSuite.class"
    )
  }

  // Test reuse of the ZipFileSystem
  test("find a second file in the same jar".ignore) {
    val path = Paths.get("munit/Assertions.class")

    assertEquals(
      FileUtils.resolvePath(path).get.toString(),
      "/munit/FunSuite.class"
    )
  }


  test("get extension of a file name") {
    val path = "foo.jar"

    assertEquals(FileUtils.getExtension(path), Some("jar"))
  }

  test("get extension of a file name with multiple dots") {
    val path = "foo.bar.jar"

    assertEquals(FileUtils.getExtension(path), Some("jar"))
  }

  test("don't get extension of a URI path without extension") {
    val path = new URI("http://foo.bar/jar").getPath

    assertEquals(FileUtils.getExtension(path), None)
  }

  test("don't get extension of path ending with dot") {
    assertEquals(FileUtils.getExtension("foo."), None)
  }

  test("handle path containing only dots") {
    assertEquals(FileUtils.getExtension("..."), None)
  }
}

package latis.util

import java.net.URI
import java.nio.file.Paths

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class FileUtilsSpec extends AnyFlatSpec {
  
  /* 
   * This is designed to find the file in the local file system, 
   * a case where tests have failed before.
   * Some testing environments may find it in a jar file. 
   * If they do, then they didn't suffer from the original problem.
   */
  "The path resolver" should "find a file in the classpath" in {
    val path = Paths.get("fdml.xsd")
    assert(FileUtils.resolvePath(path).nonEmpty)
  }
  
  it should "find a file in a jar in the classpath" in {
    val path = Paths.get("org/scalatest/flatspec/AnyFlatSpec.class")
    FileUtils.resolvePath(path).get.toString should be ("/org/scalatest/flatspec/AnyFlatSpec.class")
  }
  
  // Test reuse of the ZipFileSystem
  it should "find a second file in the same jar" in {
    val path = Paths.get("org/scalatest/Assertions.class")
    FileUtils.resolvePath(path).get.toString should be ("/org/scalatest/Assertions.class")
  }


  "getExtension" should "get extension of a file name" in {
    val path = "foo.jar"
    FileUtils.getExtension(path) should be (Some("jar"))
  }

  it should "get extension of a file name with multiple dots" in {
    val path = "foo.bar.jar"
    FileUtils.getExtension(path) should be (Some("jar"))
  }

  it should "not get extension of a URI path without extension" in {
    val path = new URI("http://foo.bar/jar").getPath
    FileUtils.getExtension(path) should be (None)
  }

  it should "handle path ending with dot" in {
    FileUtils.getExtension("foo.") should be (None)
  }

  it should "handle all dots" in {
    FileUtils.getExtension("...") should be (None)
  }
}

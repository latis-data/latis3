package latis.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import java.nio.file.Paths

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
}

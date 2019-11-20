package latis.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import java.nio.file.Paths

class FileUtilsSpec extends FlatSpec {
  
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
    val path = Paths.get("org/scalatest/FlatSpec.class")
    FileUtils.resolvePath(path).get.toString should be ("/org/scalatest/FlatSpec.class")
  }
}

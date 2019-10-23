package latis.util

import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit.rules.TemporaryFolder
import java.io.File

class TestFileUtil extends JUnitSuite {

  val _temporaryFolder = new TemporaryFolder

  @Rule
  def temporaryFolder: TemporaryFolder = _temporaryFolder

  val tempFileFoo: File = temporaryFolder.newFile("foo.fdml")
  val tempFileBar: File = temporaryFolder.newFile("bar.fdml")

  @Test
  def get_file_list(): Unit = {
    val fileList = FileUtils.getFileList(temporaryFolder.toString, ".fdml")
    assertEquals(Seq("foo", "bar"), fileList)
  }
}

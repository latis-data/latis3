package latis.input

import java.io.FileNotFoundException
import java.net.URI

import munit.CatsEffectSuite

import latis.util.LatisException

class StreamSourceSuite extends CatsEffectSuite {

  test("delay throwing an exception for file not found") {
    val s = StreamSource.getStream(new URI("file:///not/found"))

    s.compile.toList.intercept[FileNotFoundException]
  }

  test("delay throwing an exception for s3 object not found") {
    val s = StreamSource.getStream(new URI("s3://not/found"))

    s.compile.toList.intercept[LatisException]
  }
}

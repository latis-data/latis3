package latis.input

import java.net.UnknownHostException
import java.net.URI
import java.nio.file.NoSuchFileException

import munit.CatsEffectSuite

import latis.util.LatisException

class StreamSourceSuite extends CatsEffectSuite {

  test("Delay throwing an exception for file not found") {
    StreamSource.getStream(new URI("file:///not/found"))
      .compile.toList.intercept[NoSuchFileException]
  }

  test("Delay throwing an exception for 404".ignore) {
    StreamSource.getStream(new URI("https://github.com/latis-data/not/found"))
      .compile.toList.intercept[NoSuchFileException]
  }

  test("Delay throwing an exception for unknown host") {
    StreamSource.getStream(new URI("https://not.found"))
      .compile.toList.intercept[UnknownHostException]
  }

  test("Redirect") {
    StreamSource.getStream(new URI("http://github.com/"))
      .compile.toList.map { l =>
        assert(l.nonEmpty)
      }
  }

  test("Delay throwing an exception for unsupported URI scheme") {
    StreamSource.getStream(new URI("foo://not.found"))
      .compile.toList.intercept[LatisException]
  }

}

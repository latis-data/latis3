package latis.input

import java.io.FileNotFoundException
import java.net.URI

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec

import latis.util.LatisException

class StreamSourceSpec extends AnyFlatSpec {

  "A StreamSource" should "delay throwing an exception for file not found" in {
    val s = StreamSource.getStream(new URI("file:///not/found"))
    try {
      s.compile.toList.unsafeRunSync()
      fail("Exception not thrown")
    } catch {
      case _: FileNotFoundException => //pass
      case t: Throwable => fail(s"Unexpected exception: $t")
    }
  }

  it should "delay throwing an exception for s3 object not found" in {
    val s = StreamSource.getStream(new URI("s3://not/found"))
    try {
      s.compile.toList.unsafeRunSync()
      fail("Exception not thrown")
    } catch {
      case _: LatisException => //pass
      case t: Throwable => fail(s"Unexpected exception: $t")
    }
  }
}

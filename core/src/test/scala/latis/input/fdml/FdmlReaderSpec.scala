package latis.input.fdml

import java.net.URI

import org.scalatest.Inside._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.model._
import latis.util.StreamUtils

class FdmlReaderSpec extends AnyFlatSpec {

  "An FDML Reader" should "create a dataset from FDML" in {
    val ds = FdmlReader.read(new URI("datasets/data.fdml"), true)

    inside(ds.model) { case Function(domain, range) =>
      domain shouldBe a [latis.time.Time]
      range shouldBe a [Tuple]

      inside(range) { case Tuple(s1, s2, s3) =>
        s1 shouldBe a [Scalar]
        s2 shouldBe a [Scalar]
        s3 shouldBe a [Scalar]
      }
    }

    inside(StreamUtils.unsafeHead(ds.samples)) {
      case Sample(DomainData(Number(t)), RangeData(Integer(b), Real(c), Text(d))) =>
        assert(t == 0)
        assert(b == 1)
        assert(c == 1.1)
        assert(d == "a")
    }
  }

  it should "validate an FDML file on load" in {
    val err = intercept[Exception] {
      FdmlReader.read(new URI("datasets/invalid.fdml"), true)
    }

    err.getMessage should include ("'{source}' is expected")
  }

  it should "apply operations to the returned dataset" in {
    val ds = FdmlReader.read(new URI("datasets/dataWithOperations.fdml"), true)

    inside(StreamUtils.unsafeHead(ds.samples)) {
      case Sample(DomainData(), RangeData(Integer(b), Real(c))) =>
        assert(b == 2)
        assert(c == 2.2)
    }
    inside(ds.model) {
      case Function(_, range) =>
        assert(range.getScalars.map(_.id.asString) == List("b", "Istanbul"))
    }
  }
}

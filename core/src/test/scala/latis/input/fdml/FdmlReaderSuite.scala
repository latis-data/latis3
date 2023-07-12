package latis.input.fdml

import java.net.URI

import munit.CatsEffectSuite

import latis.data._
import latis.model._

class FdmlReaderSuite extends CatsEffectSuite {

  test("create a dataset from FDML") {
    val ds = FdmlReader.read(new URI("datasets/data.fdml"), true)

    ds.model match {
      case Function(domain, range) =>
        assert(domain.isInstanceOf[latis.time.Time])
        assert(range.isInstanceOf[Tuple])

        range match {
          case Tuple(s1, s2, s3) =>
            assert(s1.isInstanceOf[Scalar])
            assert(s2.isInstanceOf[Scalar])
            assert(s3.isInstanceOf[Scalar])
          case _ => fail("unexpected model")
        }
      case _ => fail("unexpected model")
    }

    val expected = Option(Sample(List(0), List(1, 1.1, "a")))
    ds.samples.take(1).compile.last.assertEquals(expected)
  }

  test("validate an FDML file on load") {
    val err = intercept[Exception] {
      FdmlReader.read(new URI("datasets/invalid.fdml"), true)
    }

    assert(err.getMessage.contains("'{source}' is expected"))
  }

  test("apply operations to the returned dataset") {
    val ds = FdmlReader.read(new URI("datasets/dataWithOperations.fdml"), true)

    val expected = Option(Sample(List(), List(2, 2.2)))
    ds.samples.take(1).compile.last.assertEquals(expected)

    ds.model match {
      case Function(_, range) =>
        assert(range.getScalars.map(_.id.asString) == List("b", "Istanbul"))
      case _ => fail("unexpected model")
    }
  }
}

package latis.input
package fdml

import java.net.URI

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data._
import latis.util.StreamUtils

class FdmlReaderSpec extends FlatSpec {

  "An FDML Reader" should "create a dataset from FDML" in {
    val ds = FdmlReader.read(new URI("datasets/data.fdml"), true)

    StreamUtils.unsafeHead(ds.samples) match {
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
}

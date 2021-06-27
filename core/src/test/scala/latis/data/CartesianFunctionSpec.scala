package latis.data

import Data._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside


class CartesianFunctionSpec extends AnyFlatSpec {

  "A CartesianFunction" should "evaluate" in {
    val xs: IndexedSeq[Datum] = Vector(1.1, 2.2, 3.3).map(DoubleValue)
    val x: Datum = DoubleValue(2.2)
    CartesianFunction1D.fromData(xs, xs).map { f =>
      inside(f.eval(DomainData(x))) {
        case Right(RangeData(Number(d))) =>
          d should be (2.2)
      }
    }
  }
}

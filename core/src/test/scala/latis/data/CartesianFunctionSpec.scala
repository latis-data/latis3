package latis.data

import Data._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._


class CartesianFunctionSpec extends AnyFlatSpec {

  "A CartesianFunction" should "evaluate" in {
    val xs: IndexedSeq[Datum] = Vector(1.1, 2.2, 3.3).map(DoubleValue)
    val x: Datum = DoubleValue(2.2)
    CartesianFunction1D.fromData(xs, xs).map { f =>
      f.eval(DomainData(x)) match {
        case Right(RangeData(Number(d))) =>
          d should be (2.2)
      }
    }
  }
}

package latis.data

import Data._

import org.scalatest.FlatSpec
import org.scalatest.Matchers._


class CartesianFunctionSpec extends FlatSpec {

  "A CartesianFunction" should "evaluate" in {
    val xs: IndexedSeq[Datum] = Vector(1.1, 2.2, 3.3).map(DoubleValue)
    val x: Datum = DoubleValue(2.2)
    CartesianFunction1D.fromData(xs, xs).map { f =>
      f(DomainData(x)) match {
        case Right(RangeData(Number(d))) =>
          d should be (2.2)
      }
    }
  }
}

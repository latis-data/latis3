package latis.tests

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.CartesianFunction1D
import latis.data.Datum
import latis.data.DomainData
import latis.data.Number
import latis.data.RangeData


class CartesianFunctionSpec extends FlatSpec {

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

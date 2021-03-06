package latis.ops

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside

import latis.data._
import latis.dataset.Dataset
import latis.dsl._

class TakeSpec extends AnyFlatSpec {

  "The Take Operation" should "return the first n samples of a simple dataset" in {
    val ds: Dataset = DatasetGenerator("a -> b")
    val dsTake      = ds.withOperation(Take(2))
    val samples     = dsTake.samples.compile.toList.unsafeRunSync()
    samples should be(
      List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(1))
      )
    )
  }

  it should "return the first n samples of a dataset with a nested function" in {
    val ds      = DatasetGenerator("(a, b) -> c").curry(1).withOperation(Take(1))
    val samples = ds.samples.compile.toList.unsafeRunSync()
    val sf = SampledFunction(
      Seq(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2))
      )
    )
    samples should be(List(Sample(DomainData(0), Seq(sf))))
  }

  it should "treat a negative number as 0" in {
    inside(Take(-1)) {
      case Take(n) => assert(n == 0)
    }
  }
}

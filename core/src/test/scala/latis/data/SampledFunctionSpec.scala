package latis.data

import latis.dataset.Dataset
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class SampledFunctionSpec extends FlatSpec {



  val dataSf: SampledFunction = SampledFunction(Dataset.fromName("data").samples)


  "A join on a Sampled Function" should "join the range when the domain is the same" in {
    val sample1: Sample = Sample(List(0), List(7))
    val sample2: Sample = Sample(List(1), List(8))
    val sample3: Sample = Sample(List(2), List(9))
    val sampledF: SampledFunction = SampledFunction(List(sample1, sample2, sample3))

    val jointSf: SampledFunction = sampledF.join(dataSf)
    val jointSfList = jointSf.streamSamples.compile.toList.unsafeRunSync()

    val expected = List(
      Sample(DomainData(0), RangeData(7, 1, 1.1, "a")),
      Sample(DomainData(1), RangeData(8, 2, 2.2, "b")),
      Sample(DomainData(2), RangeData(9, 4, 3.3, "c")),
    )

    jointSfList should be(expected)
  }

  it should "fail if the domains are not the same" in {
    assertThrows[Exception] {
      val sampledF2: SampledFunction = SampledFunction(
        List(Sample(List(42), List(9000))))
      val failJoin = dataSf.join(sampledF2)
    }
  }

}

package latis.data

import latis.dataset.Dataset
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class SampledFunctionSpec extends FlatSpec {

  val sample1: Sample = Sample(List(0), List(7))
  val sample2: Sample = Sample(List(1), List(8))
  val sample3: Sample = Sample(List(2), List(9))
  val sampledF: SampledFunction = SampledFunction(List(sample1, sample2, sample3))
  val dataSf: SampledFunction = SampledFunction(Dataset.fromName("data").samples)


  "A Sampled Function" should "be able to join another sampled function" in {
    val jointSf: SampledFunction = sampledF.join(dataSf)
    val jointSfList = jointSf.streamSamples.compile.toList.unsafeRunSync()

  jointSfList.length should be(3)
    jointSfList.map {
      case Sample(_, r) => r.length should be(4)
    }
  }
}

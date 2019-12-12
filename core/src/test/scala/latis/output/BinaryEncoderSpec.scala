package latis.output

import scodec._
import scodec.codecs.implicits._
import scodec.{Encoder => SEncoder}
import latis.data.{DomainData, RangeData, Sample}
import latis.dataset.Dataset
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class BinaryEncoderSpec extends FlatSpec {

  /**
   * Instance of BinaryEncoder for testing.
   */
  val enc = new BinaryEncoder
  val ds: Dataset = Dataset.fromName("data")

  "A Binary encoder" should "encode a dataset to binary" in {
    val encodedList = enc.encode(ds).compile.toList.unsafeRunSync()

    val expected = List(
      SEncoder.encode(0).require ++
        SEncoder.encode(1).require ++
        SEncoder.encode(1.1).require ++
        SEncoder.encode("a").require,
      SEncoder.encode(1).require ++
        SEncoder.encode(2).require ++
        SEncoder.encode(2.2).require ++
        SEncoder.encode("b").require,
      SEncoder.encode(2).require ++
        SEncoder.encode(4).require ++
        SEncoder.encode(3.3).require ++
        SEncoder.encode("c").require
    )

    encodedList should be(expected)
  }

  it should "encode a Sample to binary" in {
    val sample = Sample(DomainData(0), RangeData(1, 1.1, "a"))
    val expected = Attempt.successful(
      SEncoder.encode(0).require ++
        SEncoder.encode(1).require ++
        SEncoder.encode(1.1).require ++
        SEncoder.encode("a").require)

    enc.sampleEncoder.encode(sample) should be(expected)
  }

  // TODO: test encoder for Data for each datatype.
}

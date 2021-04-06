package latis.output

import java.nio.file.Paths

import scodec._
import scodec.bits._
import scodec.codecs.implicits._
import scodec.{Encoder => SEncoder}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import scodec.bits.BitVector

import latis.catalog.FdmlCatalog
import latis.data.Data._
import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.dataset.Dataset
import latis.metadata.Metadata
import latis.model.Function
import latis.model.Scalar
import latis.model.Tuple
import latis.util.Identifier.IdentifierStringContext

class BinaryEncoderSpec extends AnyFlatSpec {
  /**
   * Instance of BinaryEncoder for testing.
   */
  val enc = new BinaryEncoder
  val ds: Dataset = {
    val catalog = FdmlCatalog.fromClasspath(
      getClass().getClassLoader(),
      Paths.get("datasets"),
      validate = false
    )

    catalog.findDataset(id"data2").unsafeRunSync().getOrElse {
      fail("Unable to find dataset")
    }
  }

  "A Binary encoder" should "encode a dataset to binary" in {
    val encodedList = enc.encode(ds).compile.toList.unsafeRunSync()

    val expected = List(
      SEncoder.encode(3).require ++
        SEncoder.encode(6).require ++
        SEncoder.encode(4.4).require ++
        BitVector(hex"64"),
      SEncoder.encode(4).require ++
        SEncoder.encode(8).require ++
        SEncoder.encode(5.5).require ++
        BitVector(hex"65"),
      SEncoder.encode(5).require ++
        SEncoder.encode(10).require ++
        SEncoder.encode(6.6).require ++
        BitVector(hex"66")
    )

    encodedList should be(expected)
  }

  it should "encode a Sample to binary" in {
    val sample = Sample(DomainData(0), RangeData(1, 1.1, "a"))
    val model = Function(
      Scalar(Metadata("id" -> "d", "type" -> "int")),
      Tuple(
        Scalar(Metadata("id" -> "r0", "type" -> "int")),
        Scalar(Metadata("id" -> "r1", "type" -> "double")),
        Scalar(Metadata("id" -> "r2", "type" -> "string", "size" -> "2"))
      )
    )

    val expected = Attempt.successful(
      SEncoder.encode(0).require ++
        SEncoder.encode(1).require ++
        SEncoder.encode(1.1).require ++
        BitVector(hex"6100")
    )

    enc.sampleEncoder(model).encode(sample) should be(expected)
  }

  it should "encode Data to binary" in {
    val dataToEncode = List(
      true: BooleanValue,
      (1: Byte): ByteValue,
      (2: Short): ShortValue,
      3: IntValue,
      (4: Long): LongValue,
      (5: Float): FloatValue,
      6.6: DoubleValue,
//      Array(7: Byte, 8: Byte): BinaryValue,
      "foo": StringValue
    )
    val scalars = List(
      Scalar(Metadata("id" -> "a", "type" -> "boolean")),
      Scalar(Metadata("id" -> "a", "type" -> "byte")),
      Scalar(Metadata("id" -> "a", "type" -> "short")),
      Scalar(Metadata("id" -> "a", "type" -> "int")),
      Scalar(Metadata("id" -> "a", "type" -> "long")),
      Scalar(Metadata("id" -> "a", "type" -> "float")),
      Scalar(Metadata("id" -> "a", "type" -> "double")),
      Scalar(Metadata("id" -> "a", "type" -> "string", "size" -> "4"))
    )

    val expected = List(
      Attempt.successful(BitVector(hex"ff")),
      Attempt.successful(BitVector(hex"01")),
      Attempt.successful(BitVector(hex"0002")),
      Attempt.successful(BitVector(hex"00000003")),
      Attempt.successful(BitVector(hex"0000000000000004")),
      Attempt.successful(BitVector(hex"40a00000")),
      Attempt.successful(BitVector(hex"401a666666666666")),
      Attempt.successful(BitVector(hex"666f6f00"))
    )

    scalars.zip(dataToEncode).map {
      case (s, d) => enc.dataCodec(s).encode(d)
    } should be(expected)
  }
}

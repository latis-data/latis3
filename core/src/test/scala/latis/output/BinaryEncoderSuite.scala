package latis.output

import munit.CatsEffectSuite
import scodec._
import scodec.bits.BitVector
import scodec.bits._
import scodec.codecs.implicits._
import scodec.{Encoder => SEncoder}

import latis.data.Data._
import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.dataset.Dataset
import latis.dsl.DatasetGenerator
import latis.metadata.Metadata
import latis.model._
import latis.ops.Projection
import latis.util.Identifier.IdentifierStringContext

class BinaryEncoderSuite extends CatsEffectSuite {

  /** Instance of BinaryEncoder for testing. */
  val enc = new BinaryEncoder

  test("encode a dataset to binary") {
    val ds: Dataset = DatasetGenerator("x -> (a: int, b: double)")

    val bitVec =
      SEncoder.encode(0).require ++
        SEncoder.encode(0).require ++
        SEncoder.encode(0.0).require ++
      SEncoder.encode(1).require ++
        SEncoder.encode(1).require ++
        SEncoder.encode(1.0).require ++
      SEncoder.encode(2).require ++
        SEncoder.encode(2).require ++
        SEncoder.encode(2.0).require
    val expected: List[Byte] = bitVec.toByteArray.toList

    enc.encode(ds).compile.toList.map { encodedList =>
      assertEquals(encodedList, expected)
    }
  }

  test("encode a dataset with an Index variable") {
    val ds: Dataset = DatasetGenerator("x -> (a: int, b: double)")
      .withOperation(Projection.fromExpression("a, b").getOrElse(fail("projection not generated")))

    val bitVec =
        SEncoder.encode(0).require ++
        SEncoder.encode(0.0).require ++
        SEncoder.encode(1).require ++
        SEncoder.encode(1.0).require ++
        SEncoder.encode(2).require ++
        SEncoder.encode(2.0).require
    val expected: List[Byte] = bitVec.toByteArray.toList

    enc.encode(ds).compile.toList.map { encodedList =>
      assertEquals(encodedList, expected)
    }
  }

  test("encode Data to binary") {
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
      Scalar(id"a", BooleanValueType),
      Scalar(id"a", ByteValueType),
      Scalar(id"a", ShortValueType),
      Scalar(id"a", IntValueType),
      Scalar(id"a", LongValueType),
      Scalar(id"a", FloatValueType),
      Scalar(id"a", DoubleValueType),
      Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "string", "size" -> "4")).getOrElse(fail("scalar not generated"))
    )

    val expected = List(
      Attempt.successful(BitVector(hex"ff")),
      Attempt.successful(BitVector(hex"01")),
      Attempt.successful(BitVector(hex"0002")),
      Attempt.successful(BitVector(hex"00000003")),
      Attempt.successful(BitVector(hex"0000000000000004")),
      Attempt.successful(BitVector(hex"40a00000")),
      Attempt.successful(BitVector(hex"401a666666666666")),
      Attempt.successful(BitVector(hex"666f6f00")),
    )

    assertEquals(
      scalars.zip(dataToEncode).map {
        case (s, d) => enc.dataCodec(s).encode(d)
      },
      expected
    )
  }

  test("encode a binary value") {
    val scalar = Scalar(id"a", BinaryValueType)
    val codec = enc.dataCodec(scalar)
    val bin = BinaryValue("foo".getBytes)
    codec.encode(bin) match {
      case Attempt.Successful(bv: BitVector) =>
        assertEquals(bv.toByteArray.map(_.toChar).mkString,"foo")
      case _ => fail("BitVector not generated")
    }
  }

  test("decode a Sample from binary") {
    val sample = Sample(DomainData(0), RangeData(1, 1.1, "a"))
    val model = Function.from(
      Scalar(id"d", IntValueType),
      Tuple.fromElements(
        Scalar(id"r1", IntValueType),
        Scalar(id"r2", DoubleValueType),
        Scalar.fromMetadata(Metadata("id" -> "r2", "type" -> "string", "size" -> "2")).getOrElse(fail("scalar not generated"))
      ).getOrElse(fail("tuple not generated"))
    ).getOrElse(fail("model not generated"))

    val encoded =
      SEncoder.encode(0).require ++
        SEncoder.encode(1).require ++
        SEncoder.encode(1.1).require ++
        BitVector(hex"6100")

    val decoded = enc.sampleCodec(model).decode(encoded) match {
      case Attempt.Successful(DecodeResult(value, _)) => value
      case _ => fail("Failed to decode")
    }
    assertEquals(decoded, sample)
  }

  test("encode a Sample to binary") {
    val sample = Sample(DomainData(0), RangeData(1, 1.1, "a"))
    val model = Function.from(
      Scalar(id"d", IntValueType),
      Tuple.fromElements(
        Scalar(id"r1", IntValueType),
        Scalar(id"r2", DoubleValueType),
        Scalar.fromMetadata(Metadata("id" -> "r2", "type" -> "string", "size" -> "2")).getOrElse(fail("scalar not generated"))
      ).getOrElse(fail("tuple not generated"))
    ).getOrElse(fail("model not generated"))

    val expected = Attempt.successful(
      SEncoder.encode(0).require ++
        SEncoder.encode(1).require ++
        SEncoder.encode(1.1).require ++
        BitVector(hex"6100"))

    assertEquals(enc.sampleCodec(model).encode(sample), expected)
  }
}

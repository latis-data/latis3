package latis.ops

import munit.CatsEffectSuite
import scodec.bits.*

import latis.data.*
import latis.dataset.TappedDataset
import latis.metadata.Metadata
import latis.model.*
import latis.ops.ConvertBinary.*
import latis.util.Identifier.*

class ConvertBinarySuite extends CatsEffectSuite {

  private lazy val dataset = new TappedDataset(
    Metadata(id"test"),
    Scalar.fromMetadata(Metadata(
      "id"        -> "bytes",
      "type"      -> "binary",
      "mediaType" -> "example"
    )).getOrElse(fail("Invalid scalar")),
    Data.BinaryValue(hex"cafebabe".toArray)
  )

  test("toBase64") {
    val ds = dataset.withOperation(ConvertBinary(id"bytes", Base64))
    ds.samples.compile.toList.map { list =>
      list.head match {
        case Sample(_, RangeData(Text(s))) => assertEquals(s, "yv66vg==")
        case _ => fail("Bad sample")
      }
    }
  }

  test("toHex") {
    val ds = dataset.withOperation(ConvertBinary(id"bytes", Base16))
    ds.samples.compile.toList.map { list =>
      list.head match {
        case Sample(_, RangeData(Text(s))) => assertEquals(s, "cafebabe")
        case _ => fail("Bad sample")
      }
    }
  }

  test("toBin") {
    val ds = dataset.withOperation(ConvertBinary(id"bytes", Base2))
    ds.samples.compile.toList.map { list =>
      list.head match {
        case Sample(_, RangeData(Text(s))) => assertEquals(s, "11001010111111101011101010111110")
        case _ => fail("Bad sample")
      }
    }
  }

  test("mediaType") {
    val ds = dataset.withOperation(ConvertBinary(id"bytes", Base64))
    ds.model.findVariable(id"bytes") match {
      case Some(s: Scalar) => s.metadata.getProperty("mediaType") match {
        case Some(mt) => assertEquals(mt, "example;base64")
        case _ => fail("No mediaType")
      }
      case _ => fail("Variable not found")
    }
  }
}

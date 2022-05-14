package latis.dataset

import munit.CatsEffectSuite

import latis.data.Data._
import latis.data.Sample
import latis.data._
import latis.dsl._
import latis.metadata.Metadata
import latis.model._
import latis.ops.Append
import latis.ops.Rename
import latis.ops.Selection
import latis.util.Identifier.IdentifierStringContext
import latis.util.dap2.parser.ast

class CompositeDatasetSuite extends CatsEffectSuite {

  private lazy val ds1 = {
    val metadata = Metadata(id"test1")

    val model = ModelParser.unsafeParse("time: int -> flux: double")

    val data = {
      val samples = List(
        Sample(DomainData(1), RangeData(1.2)),
        Sample(DomainData(2), RangeData(2.4))
      )
      SampledFunction(samples)
    }

    new MemoizedDataset(metadata, model, data)
  }

  private lazy val ds2 = {
    val metadata = Metadata(id"test2")

    val model = ModelParser.unsafeParse("time: int -> flux: double")

    val data = {
      val samples = List(
        Sample(DomainData(3), RangeData(3.6)),
        Sample(DomainData(4), RangeData(4.8))
      )
      SampledFunction(samples)
    }

    new MemoizedDataset(metadata, model, data)
  }

  private lazy val compDs = CompositeDataset(id"test", Append(), ds1, ds2)

  test("provide a composite model") {
    compDs.model match {
      case Function(t: Scalar, f: Scalar) =>
        assertEquals(t.valueType, IntValueType)
        assertEquals(t.id.asString, "time")
        assertEquals(f.valueType, DoubleValueType)
        assertEquals(f.id.asString, "flux")
      case _ => fail("model not of correct function type")
    }
  }

  test("provide a stream of samples") {
    compDs.samples.compile.toList.map {
      case Seq(s1, s2, s3, s4) =>
        assertEquals(s1, Sample(DomainData(1), RangeData(1.2)))
        assertEquals(s2, Sample(DomainData(2), RangeData(2.4)))
        assertEquals(s3, Sample(DomainData(3), RangeData(3.6)))
        assertEquals(s4, Sample(DomainData(4), RangeData(4.8)))
      case _ => fail("sequence format not correct")
    }
  }

  test("apply an operation that mutates data") {
    val select = Selection(id"time", ast.Gt, "1")
    val compDs2 = compDs.withOperation(select)
    compDs2.samples.compile.toList.map {
      case Seq(s2, s3, s4) =>
        assertEquals(s2, Sample(DomainData(2), RangeData(2.4)))
        assertEquals(s3, Sample(DomainData(3), RangeData(3.6)))
        assertEquals(s4, Sample(DomainData(4), RangeData(4.8)))
      case _ => fail("sequence format not correct")
    }
  }

  test("apply an operation that mutates the model") {
    val rename = Rename(id"flux", id"foo")
    val compDs2 = compDs.withOperation(rename)
    assertEquals(compDs2.model.toString, ModelParser.unsafeParse("time: int -> foo: double").toString)
  }

}

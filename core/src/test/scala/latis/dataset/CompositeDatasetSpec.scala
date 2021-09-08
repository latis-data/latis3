package latis.dataset

import org.scalatest.Inside._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.data.Data._
import latis.data.Sample
import latis.dsl._
import latis.metadata.Metadata
import latis.model._
import latis.ops.Append
import latis.ops.Rename
import latis.ops.Selection
import latis.util.Identifier.IdentifierStringContext
import latis.util.StreamUtils
import latis.util.dap2.parser.ast

class CompositeDatasetSpec extends AnyFlatSpec {

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

  "A dataset" should "provide a composite model" in {
    inside(compDs.model) {
      case Function(t: Scalar, f: Scalar) =>
        assert(t.valueType == IntValueType)
        assert(t.id.asString == "time")
        assert(f.valueType == DoubleValueType)
        assert(f.id.asString == "flux")
    }
  }

  it should "provide a stream of samples" in {
    inside(StreamUtils.unsafeStreamToSeq(compDs.samples)) {
      case Seq(s1, s2, s3, s4) =>
        s1 should be (Sample(DomainData(1), RangeData(1.2)))
        s2 should be (Sample(DomainData(2), RangeData(2.4)))
        s3 should be (Sample(DomainData(3), RangeData(3.6)))
        s4 should be (Sample(DomainData(4), RangeData(4.8)))
      case _ => fail()
    }
  }

  it should "apply an operation that mutates data" in {
    val select = Selection(id"time", ast.Gt, "1")
    val compDs2 = compDs.withOperation(select)
    inside(StreamUtils.unsafeStreamToSeq(compDs2.samples)) {
      case Seq(s2, s3, s4) =>
        s2 should be (Sample(DomainData(2), RangeData(2.4)))
        s3 should be (Sample(DomainData(3), RangeData(3.6)))
        s4 should be (Sample(DomainData(4), RangeData(4.8)))
      case _ => fail()
    }
  }

  it should "apply an operation that mutates the model" in {
    val rename = Rename(id"flux", id"foo")
    val compDs2 = compDs.withOperation(rename)
    compDs2.model.toString should be (ModelParser.unsafeParse("time: int -> foo: double").toString)
  }

}

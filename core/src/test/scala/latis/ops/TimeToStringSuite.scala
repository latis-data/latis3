package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.*
import latis.util.Identifier.*

class TimeToStringSuite extends CatsEffectSuite {

  def mockDataset: MemoizedDataset = {
    val metadata: Metadata = Metadata(id"MockDataset") + ("title" -> "Mock Dataset")
    val model: DataType = {
      Scalar.fromMetadata(Metadata(id"time") + ("type" -> "int") + ("units" -> "days since 1975")).fold(fail("Failed to construct model", _), identity)
    }
    
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(), RangeData(1955001)),
        Sample(DomainData(), RangeData(2004322)),
        Sample(DomainData(), RangeData(2026102)),
        Sample(DomainData(), RangeData(2025120))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  test("convert a time int to a string") {
    val ds = mockDataset.withOperation(TimeToString(id"time"))

    ds.model match {
      case s: Scalar =>
        assertEquals(s.units, Option("yyyyDDD"))
        assertEquals(s.valueType, StringValueType)
      case _ => fail("unexpected model")
    }

    ds.samples.compile.toList.assertEquals(
      List(
        Sample(List(), List("1955001")),
        Sample(List(), List("2004322")),
        Sample(List(), List("2026102")),
        Sample(List(), List("2025120"))
      )
    )
  }
}

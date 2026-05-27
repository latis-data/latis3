package latis.ops

import munit.CatsEffectSuite

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.MemoizedDataset
import latis.dsl.*
import latis.metadata.Metadata
import latis.util.Identifier.id

class OnChangeSuite extends CatsEffectSuite {

  private val model = ModelParser.parse("x -> y").fold(throw _, identity)

  // samples with some non-changing range values
  private val samples = List(
    Sample(DomainData(0), RangeData(0)),
    Sample(DomainData(1), RangeData(0)),
    Sample(DomainData(2), RangeData(0)),
    Sample(DomainData(3), RangeData(1)),
    Sample(DomainData(4), RangeData(1)),
    Sample(DomainData(5), RangeData(1)),
  )

  private val ds = new MemoizedDataset(
    Metadata(id"test"),
    model,
    SampledFunction(samples)
  )

  test("on change, keeping last unchanged sample") {
    val oc = OnChange.fromArgs(List("y"))
      .fold(_ => fail("Failed to make OnChange"), identity)
    val samples = ds.withOperation(oc).samples.compile.toList
    samples.assertEquals(
      List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(3), RangeData(1)),
        Sample(DomainData(5), RangeData(1)),
      )
    )
  }

  test("on change keeps last sample when changed") {
    val oc = OnChange.fromArgs(List("y"))
      .fold(_ => fail("Failed to make OnChange"), identity)
    val samples = List(
      Sample(DomainData(0), RangeData(0)),
      Sample(DomainData(1), RangeData(1)),
      Sample(DomainData(2), RangeData(2))
    )

    val ds = new MemoizedDataset(
      Metadata(id"test"),
      model,
      SampledFunction(samples)
    )
    ds.withOperation(oc).samples.compile.toList.assertEquals(
      List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2))
      )
    )
  }

  // Duplicate domain values is not allowed in the FDM,
  // but this shows that it can fix "broken" datasets
  // that we have no control of.
  test("duplicate domain value") {
    val oc = OnChange.fromArgs(List("x"))
      .fold(_ => fail("Failed to make OnChange"), identity)
    val ds = MemoizedDataset(
      Metadata(id"test"),
      model,
      SampledFunction(samples :+ Sample(DomainData(5), RangeData(2)))
    )
    ds.withOperation(oc).samples.compile.toList.map(_.length)
      .assertEquals(7)
  }

}

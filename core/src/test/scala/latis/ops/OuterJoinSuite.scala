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

class OuterJoinSuite extends CatsEffectSuite {

  // Prevent test from timing out so we can run in debugger
  import scala.concurrent.duration.DurationInt
  override val munitIOTimeout = 5.minutes

  private lazy val ds1 = {
    val metadata = Metadata(id"test1")
    val model = ModelParser.unsafeParse("x: int -> a: double")
    val samples = List(
      Sample(DomainData(1), RangeData(1.2)),
      Sample(DomainData(2), RangeData(2.4)),
      Sample(DomainData(3), RangeData(3.6))
    )
    new MemoizedDataset(metadata, model, SampledFunction(samples))
  }

  private lazy val ds2 = {
    val metadata = Metadata(id"test2")
    val model = ModelParser.unsafeParse("x: int -> b: double")
    val samples = List(
      Sample(DomainData(2), RangeData(2.4)),
      Sample(DomainData(4), RangeData(4.8))
    )
    new MemoizedDataset(metadata, model, SampledFunction(samples))
  }

  test("outer join") {
    val ds = OuterJoin().combine(ds1, ds2)
    ds.show()
  }
}

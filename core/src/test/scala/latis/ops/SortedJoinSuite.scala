package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dataset.MemoizedDataset
import latis.dsl.*
import latis.metadata.Metadata
import latis.util.Identifier.id

class SortedJoinSuite extends CatsEffectSuite {

  private val model = ModelParser.unsafeParse("x: int -> a: double")

  private lazy val ds1 = {
    val metadata = Metadata(id"test1")
    val samples = List(
      Sample(DomainData(1), RangeData(1.2)),
      Sample(DomainData(2), RangeData(2.4)),
      Sample(DomainData(3), RangeData(3.6))
    )
    new MemoizedDataset(metadata, model, SampledFunction(samples))
  }

  private lazy val ds2 = {
    val metadata = Metadata(id"test2")
    val samples = List(
      Sample(DomainData(2), RangeData(2.5)),
      Sample(DomainData(4), RangeData(4.8))
    )
    new MemoizedDataset(metadata, model, SampledFunction(samples))
  }

  test("sorted join") {
    (SortedJoin().combine(ds1, ds2) match {
      case Right(ds) => ds.samples.map {
        case Sample(_, RangeData(d: Data.DoubleValue)) => d.value
      }
      case _ => fail("Failed to zip")
    }).compile.toList.map { ds =>
      assertEquals(ds, List(1.2, 2.4, 3.6, 4.8))
    }
  }
}

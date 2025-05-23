package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dataset.*
import latis.dsl.*
import latis.metadata.Metadata
import latis.util.Identifier.*

class GroupByBinSuite extends CatsEffectSuite {

  private lazy val model = ModelParser.unsafeParse("x -> (a, b)")

  private val samples = List(
    Sample(DomainData(0), RangeData(1, 1)),
    Sample(DomainData(1), RangeData(2, 4)),
    Sample(DomainData(2), RangeData(3, 9)),
    Sample(DomainData(3), RangeData(4, 16)),
    Sample(DomainData(4), RangeData(5, 25)),
    Sample(DomainData(5), RangeData(6, 36)),
    Sample(DomainData(6), RangeData(7, 49)),
    Sample(DomainData(7), RangeData(8, 64)),
    Sample(DomainData(8), RangeData(9, 81)),
    Sample(DomainData(9), RangeData(10, 100)),
  )

  private lazy val data = SampledFunction(samples)

  private lazy val ds = new MemoizedDataset(Metadata(id"test"), model, data)

  test("group by bin") {
    ds.withOperation(GroupByBinWidth(3.0).fold(e => fail(e.getMessage), identity))
      .samples.compile.lastOrError.flatMap {
        case Sample(DomainData(Number(x)), RangeData(f)) =>
          assertEquals(x, 9.0)
          f.samples.compile.toList.assertEquals(
            List(Sample(List(9), List(10, 100)))
          )
      }
  }

  test("group by bin with count") {
    ds.withOperation(GroupByBinWidth(3.0, CountAggregation2()).fold(e => fail(e.getMessage), identity))
      .samples.head.compile.toList.map {
        case List(Sample(_, RangeData(Integer(cnt)))) =>
          assertEquals(cnt, 3L)
        case _ => fail("Bad sample")
      }
  }

  test("empty dataset") {
    val gb = GroupByBinWidth(3.0).fold(e => fail(e.getMessage), identity)
    ds.withOperation(Take(0))
      .withOperation(gb)
      .samples.compile.count.map { cnt =>
        assertEquals(cnt, 0L)
      }
  }

  test("non-positive bin width") {
    assert(GroupByBinWidth(0).isLeft)
  }

  test("NaN bin width") {
    assert(GroupByBinWidth(Double.NaN).isLeft)
  }

  test("from no args") {
    assert(GroupByBinWidth.fromArgs(List()).isLeft)
  }

  test("from width arg") {
    assert(GroupByBinWidth.fromArgs(List("1")).isRight)
  }

  test("from width and aggregation args") {
    assert(GroupByBinWidth.fromArgs(List("1", "stats")).isRight)
  }

  test("from width and invalid aggregation args") {
    assert(GroupByBinWidth.fromArgs(List("1", "foobar")).isLeft)
  }

  test("from too many args") {
    assert(GroupByBinWidth.fromArgs(List("1", "stats", "foobar")).isLeft)
  }
}

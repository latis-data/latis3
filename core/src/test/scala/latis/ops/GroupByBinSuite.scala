package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dataset.*
import latis.dsl.*
import latis.metadata.Metadata
import latis.model.*
import latis.time.Time
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

  private lazy val scalar    = Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "fillValue" -> "-1"))
    .fold(fail("failed to construct scalar", _), identity)
  private lazy val scalar2   = Scalar.fromMetadata(Metadata("id" -> "b", "type" -> "int", "fillValue" -> "0"))
    .fold(fail("failed to construct scalar", _), identity)
  private lazy val tuple: Tuple = Tuple.fromElements(
    scalar,
    scalar2
  ).fold(fail("failed to construct tuple", _), identity)
  private lazy val timeScalar = Time.fromMetadata(Metadata("id" -> "x", "type" -> "string", "units" -> "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .fold(fail("failed to construct scalar", _), identity)
  private lazy val model2: Function = Function.from(timeScalar, tuple)
    .fold(fail("failed to construct function", _), identity)

  private lazy val samples2 = List(
    Sample(DomainData("2018-10-22T18:17:08Z"), RangeData(1, 1)),
    Sample(DomainData("2018-10-23T00:00:00Z"), RangeData(2, 4)),
    Sample(DomainData("2018-10-24T00:00:00Z"), RangeData(3, 9)),
    Sample(DomainData("2018-10-25T00:00:00Z"), RangeData(4, 16)),
    Sample(DomainData("2018-10-26T00:00:00Z"), RangeData(5, 25)),
    Sample(DomainData("2018-10-27T00:00:00Z"), RangeData(6, 36)),
    Sample(DomainData("2018-10-28T00:00:00Z"), RangeData(7, 49)),
    Sample(DomainData("2018-10-29T00:00:00Z"), RangeData(8, 64)),
    Sample(DomainData("2018-10-30T00:00:00Z"), RangeData(9, 81)),
    Sample(DomainData("2018-10-31T00:00:00Z"), RangeData(10, 100)),
  )
  private lazy val data2 = SampledFunction(samples2)
  private lazy val ds2 = new MemoizedDataset(Metadata(id"test2"), model2, data2)

  test("group by bin") {
    ds.withOperation(GroupByBinWidth.from(3.0).fold(e => fail(e.getMessage), identity))
      .samples.compile.lastOrError.flatMap {
        case Sample(DomainData(Number(x)), RangeData(f)) =>
          assertEquals(x, 9.0)
          f.samples.compile.toList.assertEquals(
            List(Sample(List(9), List(10, 100)))
          )
      }
  }

  test("group by bin with string domain") {
    ds2.withOperation(GroupByBinWidth.from(8.64E7).fold(e => fail(e.getMessage), identity))
      .samples.compile.lastOrError.flatMap {
        case Sample(DomainData(Text(x)), RangeData(f)) =>
          assertEquals(x, "2018-10-31T00:00:00Z")
          f.samples.compile.toList.assertEquals(
            List(Sample(List("2018-10-31T00:00:00Z"), List(10, 100)))
          )
      }
  }

  test("group by bin with count") {
    ds.withOperation(GroupByBinWidth.from(3.0, CountAggregation2()).fold(e => fail(e.getMessage), identity))
      .samples.head.compile.toList.map {
        case List(Sample(_, RangeData(Integer(cnt)))) =>
          assertEquals(cnt, 3L)
        case _ => fail("Bad sample")
      }
  }

  test("empty dataset") {
    val gb = GroupByBinWidth.from(3.0).fold(e => fail(e.getMessage), identity)
    ds.withOperation(Take(0))
      .withOperation(gb)
      .samples.compile.count.map { cnt =>
        assertEquals(cnt, 0L)
      }
  }

  test("non-positive bin width") {
    assert(GroupByBinWidth.from(0).isLeft)
  }

  test("NaN bin width") {
    assert(GroupByBinWidth.from(Double.NaN).isLeft)
  }

  test("from no args") {
    assert(GroupByBinWidth.builder.build(List()).isLeft)
  }

  test("from width arg") {
    assert(GroupByBinWidth.builder.build(List("1")).isRight)
  }

  test("from width and aggregation args") {
    assert(GroupByBinWidth.builder.build(List("1", "stats")).isRight)
  }

  test("from width and invalid aggregation args") {
    assert(GroupByBinWidth.builder.build(List("1", "foobar")).isLeft)
  }

  test("from too many args") {
    assert(GroupByBinWidth.builder.build(List("1", "stats", "foobar")).isLeft)
  }
}

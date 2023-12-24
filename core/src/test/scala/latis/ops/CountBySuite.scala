package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dataset.*
import latis.dsl.*
import latis.metadata.*
import latis.util.Identifier.*

class CountBySuite extends CatsEffectSuite {

  // (x, y) -> a
  private lazy val model = ModelParser.unsafeParse("(x, y) -> a")

  private lazy val data = SeqFunction(Seq(
    Sample(DomainData(0, 10), RangeData(1)),
    Sample(DomainData(1, 11), RangeData(2)),
    Sample(DomainData(1, 12), RangeData(3)),
    Sample(DomainData(2, 10), RangeData(4)),
    Sample(DomainData(2, 11), RangeData(5)),
    Sample(DomainData(2, 12), RangeData(6)),
  ))

  private lazy val ds = new MemoizedDataset(Metadata(id"test"), model, data)

  test("count by first domain variable") {
    val cb = CountBy.fromArgs(List("x"))
      .fold(fail("failed to construct operation", _), identity)

    ds.withOperation(cb)
      .samples.compile.toList.map {
        _.collect {
          case Sample(_, RangeData(Integer(count))) => count
        }
      }.assertEquals(List(1L, 2L, 3L))
  }

  test("count by second domain variable") {
    val cb = CountBy.fromArgs(List("y"))
      .fold(fail("failed to construct operation", _), identity)

    ds.withOperation(cb)
      .samples.compile.toList.map {
        _.collect {
          case Sample(_, RangeData(Integer(count))) => count
        }
      }.assertEquals(List(2L, 2L, 2L))
  }

  test("count by range variable") {
    val cb = CountBy.fromArgs(List("a"))
      .fold(fail("failed to construct operation", _), identity)

    ds.withOperation(cb)
      .samples.compile.toList.map {
        _.collect {
          case Sample(_, RangeData(Integer(count))) => count
        }
      }.assertEquals(List(1L, 1L, 1L, 1L, 1L, 1L))
  }

}

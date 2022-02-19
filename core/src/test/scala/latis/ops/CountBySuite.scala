package latis.ops

import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.EitherValues._

import latis.data._
import latis.dataset._
import latis.dsl._
import latis.metadata._
import latis.util.Identifier.IdentifierStringContext

class CountBySuite extends AnyFunSuite {

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
    val cb = CountBy.fromArgs(List("x")).value
    val counts = ds.withOperation(cb)
      .samples.compile.toList.map {
        _.collect {
          case Sample(_, RangeData(Integer(count))) => count
        }
      }.unsafeRunSync()
    assertResult(List(1,2,3))(counts)
  }

  test("count by second domain variable") {
    val cb = CountBy.fromArgs(List("y")).value
    val counts = ds.withOperation(cb)
      .samples.compile.toList.map {
        _.collect {
          case Sample(_, RangeData(Integer(count))) => count
        }
      }.unsafeRunSync()
    assertResult(List(2,2,2))(counts)
  }

  test("count by range variable") {
    val cb = CountBy.fromArgs(List("a")).value
    val counts = ds.withOperation(cb)
      .samples.compile.toList.map {
        _.collect {
          case Sample(_, RangeData(Integer(count))) => count
        }
      }.unsafeRunSync()
    assertResult(List(1,1,1,1,1,1))(counts)
  }

}

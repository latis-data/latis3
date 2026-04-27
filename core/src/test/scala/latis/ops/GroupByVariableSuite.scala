package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.data.Data.*
import latis.dataset.*
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.model.*
import latis.util.Identifier.*

class GroupByVariableSuite extends CatsEffectSuite {
  //TODO: test more variations
  //  group by all
  //  group by none
  //  reorder

  // (x, y) -> a
  private lazy val model = ModelParser.unsafeParse("(x, y) -> a")

  private lazy val data = SeqFunction(Seq(
    Sample(DomainData(0, 10), RangeData(1)),
    Sample(DomainData(0, 11), RangeData(2)),
    Sample(DomainData(1, 10), RangeData(3)),
    Sample(DomainData(1, 11), RangeData(4)),
  ))

  private lazy val ds = new MemoizedDataset(Metadata(id"test"), model, data)
      .withOperation(GroupByVariable(id"y"))

  test("unProject the grouped variables") {
    assertEquals(ds.model.toString, "y -> x -> a")
  }

  test("group by range variables") {
    val model = ModelParser.unsafeParse("t -> (x, y, z, a)")

    val data = SeqFunction(Seq(
      Sample(DomainData(1), RangeData(3, 5, 7, 10)),
      Sample(DomainData(2), RangeData(4, 6, 8, 11)),
    ))

    val ds = new MemoizedDataset(Metadata(id"test"), model, data)
      .withOperation(GroupByVariable(id"x", id"y", id"z"))

    assertEquals(ds.model.toString, "(x, y, z) -> t -> a")
    ds.samples.compile.toList.map { s =>
      s.head match
      case Sample(
        DomainData(x: IntValue, y: IntValue, z: IntValue),
        RangeData(MemoizedFunction(ss))
      ) =>
        assertEquals(3, x.value)
        assertEquals(5, y.value)
        assertEquals(7, z.value)
        ss.head match {
          case Sample(DomainData(t: IntValue), RangeData(a: IntValue)) =>
            assertEquals(1, t.value)
            assertEquals(10, a.value)
        }
    }
  }

  test("group with index domain") {
    val model = {
      val domain = Index()
      val range = Tuple.fromElements(
        Scalar(id"x", IntValueType),
        Scalar(id"a", IntValueType),
      ).fold(throw _, identity)
      Function.from(domain, range).fold(throw _, identity)
    }

    val data = SeqFunction(Seq(
      Sample(DomainData(), RangeData(1, 10)),
      Sample(DomainData(), RangeData(2, 20)),
    ))

    val ds = new MemoizedDataset(Metadata(id"test"), model, data)
      .withOperation(GroupByVariable(id"x"))

    assertEquals(ds.model.toString, "x -> _i -> a")
    ds.samples.compile.toList.map { s =>
      s.head match
        case Sample(
          DomainData(x: IntValue),
          RangeData(MemoizedFunction(ss))
        ) =>
          assertEquals(1, x.value)
          ss.head match {
            case Sample(DomainData(), RangeData(a: IntValue)) =>
              assertEquals(10, a.value)
          }
    }
  }
}

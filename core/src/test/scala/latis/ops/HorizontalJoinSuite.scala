package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.data.Data.DoubleValue
import latis.data.Data.IntValue
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.*
import latis.util.Identifier.id

class HorizontalJoinSuite extends CatsEffectSuite {

  private lazy val ds1 = {
    val metadata = Metadata(id"test1")
    val model = {
      for {
        a <- Scalar.fromMetadata(Metadata(
          "id"        -> "a",
          "type"      -> "double",
          "fillValue" -> "-9.9"
        ))
        f <- Function.from(Scalar(id"x", IntValueType), a)
      } yield f
    }.fold(throw _, identity)
    val samples = List(
      Sample(DomainData(1), RangeData(1.2)),
      Sample(DomainData(2), RangeData(2.4)),
      Sample(DomainData(3), RangeData(3.6))
    )
    new MemoizedDataset(metadata, model, SampledFunction(samples))
  }

  private lazy val ds2 = {
    val metadata = Metadata(id"test2")
    val model = {
      for {
        b <- Scalar.fromMetadata(Metadata(
          "id" -> "b",
          "type" -> "double",
          "fillValue" -> "-9.9"
        ))
        f <- Function.from(Scalar(id"x", IntValueType), b)
      } yield f
    }.fold(throw _, identity)
    val samples = List(
      Sample(DomainData(2), RangeData(2.4)),
      Sample(DomainData(4), RangeData(4.8))
    )
    new MemoizedDataset(metadata, model, SampledFunction(samples))
  }

  test("Full outer join") {
    (HorizontalJoin().combine(ds1, ds2) match {
      case Right(ds) => ds.samples.map {
        case Sample(_, RangeData(a: Data.DoubleValue, b: Data.DoubleValue)) =>
          (a.value, b.value)
      }
      case _ => fail("Failed to join")
    }).compile.toList.map { ds =>
      assertEquals(ds, List((1.2, -9.9), (2.4, 2.4), (3.6, -9.9), (-9.9, 4.8)))
    }
  }

  test("Left outer join") {
    (HorizontalJoin(HorizontalJoinType.Left).combine(ds1, ds2) match {
      case Right(ds) => ds.samples.map {
        case Sample(_, RangeData(a: Data.DoubleValue, b: Data.DoubleValue)) =>
          (a.value, b.value)
      }
      case _ => fail("Failed to join")
    }).compile.toList.map { ds =>
      assertEquals(ds, List((1.2, -9.9), (2.4, 2.4), (3.6, -9.9)))
    }
  }

  test("Right outer join") {
    (HorizontalJoin(HorizontalJoinType.Right).combine(ds1, ds2) match {
      case Right(ds) => ds.samples.map {
        case Sample(_, RangeData(a: Data.DoubleValue, b: Data.DoubleValue)) =>
          (a.value, b.value)
      }
      case _ => fail("Failed to join")
    }).compile.toList.map { ds =>
      assertEquals(ds, List((2.4, 2.4), (-9.9, 4.8)))
    }
  }

  test("Inner join") {
    (HorizontalJoin(HorizontalJoinType.Inner).combine(ds1, ds2) match {
      case Right(ds) => ds.samples.map {
        case Sample(_, RangeData(a: Data.DoubleValue, b: Data.DoubleValue)) =>
          (a.value, b.value)
      }
      case _ => fail("Failed to join")
    }).compile.toList.map { ds =>
      assertEquals(ds, List((2.4, 2.4)))
    }
  }

  test("zero arity") {
    val a = Scalar(id"a", IntValueType)
    val b = Scalar(id"b", DoubleValueType)
    val ds1 = MemoizedDataset(
      Metadata(id"a"), a,
      SampledFunction(List(Sample(DomainData(), RangeData(IntValue(1)))))
    )
    val ds2 = MemoizedDataset(
      Metadata(id"b"), b,
      SampledFunction(List(Sample(DomainData(), RangeData(DoubleValue(1.2)))))
    )
    HorizontalJoin().combine(ds1, ds2).fold(throw _, identity).samples.map {
      case Sample(d, RangeData(a: IntValue, b: DoubleValue)) =>
        assertEquals(a.value, 1)
        assertEquals(b.value, 1.2)
    }.compile.drain
  }

  test("join with index") {
    val ds2 = {
      val metadata = Metadata(id"test2")
      val model = Function.from(Index(), Scalar(id"b", DoubleValueType)).fold(throw _, identity)
      val samples = List(
        Sample(DomainData(), RangeData(2.4)),
        Sample(DomainData(), RangeData(4.8))
      )
      new MemoizedDataset(metadata, model, SampledFunction(samples))
    }
    (HorizontalJoin().combine(ds1, ds2) match {
      case Right(ds) => ds.samples.map {
        case Sample(
          DomainData(x: IntValue), RangeData(a: Data.DoubleValue, b: Data.DoubleValue)
        ) => (x.value, a.value, b.value)
      }
      case _ => fail("Failed to join")
    }).compile.toList.map { ds =>
      assertEquals(ds, List((1, 1.2, 2.4), (2, 2.4, 4.8)))
    }
  }

  test("duplicate id previously disambiguated") {
    val m1 = Scalar(id"a_1", IntValueType)
    val m2 = Scalar(id"a", IntValueType)
    HorizontalJoin().applyToModel(m1, m2) match {
      case Right(Tuple(a: Scalar, b: Scalar)) =>
        assertEquals(a.id, id"a_1")
        assertEquals(b.id, id"a_2")
      case _ => fail("Failed to join")
    }
  }

}

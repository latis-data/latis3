package latis.ops

import fs2.Stream
import munit.CatsEffectSuite

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.dsl.*
import latis.model.Scalar
import latis.model.StringValueType
import latis.util.Identifier.id

class MaxDeltaSuite extends CatsEffectSuite {

  private val model = ModelParser.parse("x -> a")
    .fold(e => fail(e.message), identity)

  test("one spike") {
    val samples = Stream.emits(List(
      Sample(DomainData(0), RangeData(0)),
      Sample(DomainData(1), RangeData(5)),
      Sample(DomainData(2), RangeData(1)),
    ))
    MaxDelta(id"a", 2.0).pipe(model)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(2), RangeData(1)),
      )
      assertEquals(samples, expected)
    }
  }

  test("delta equals max not dropped") {
    val samples = Stream.emits(List(
      Sample(DomainData(0), RangeData(0)),
      Sample(DomainData(1), RangeData(2)),
      Sample(DomainData(2), RangeData(1)),
    ))
    MaxDelta(id"a", 2.0).pipe(model)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(2)),
        Sample(DomainData(2), RangeData(1)),
      )
      assertEquals(samples, expected)
    }
  }

  test("spike at end") {
    val samples = Stream.emits(List(
      Sample(DomainData(0), RangeData(0)),
      Sample(DomainData(1), RangeData(1)),
      Sample(DomainData(2), RangeData(5)),
    ))
    MaxDelta(id"a", 2.0).pipe(model)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(1), RangeData(1)),
      )
      assertEquals(samples, expected)
    }
  }

  // Use case not handled, only first sample kept
  test("start with spike".ignore) {
    val samples = Stream.emits(List(
      Sample(DomainData(0), RangeData(5)),
      Sample(DomainData(1), RangeData(1)),
      Sample(DomainData(2), RangeData(2)),
    ))
    MaxDelta(id"a", 2.0).pipe(model)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(1), RangeData(1)),
        Sample(DomainData(2), RangeData(2)),
      )
      assertEquals(samples, expected)
    }
  }

  test("consecutive high values") {
    val samples = Stream.emits(List(
      Sample(DomainData(0), RangeData(0)),
      Sample(DomainData(1), RangeData(5)),
      Sample(DomainData(2), RangeData(5)),
      Sample(DomainData(3), RangeData(1)),
    ))
    MaxDelta(id"a", 2.0).pipe(model)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(0), RangeData(0)),
        Sample(DomainData(3), RangeData(1)),
      )
      assertEquals(samples, expected)
    }
  }

  test("invalid non-numeric variable") {
    val model = Scalar(id"a", StringValueType)
    assert(MaxDelta(id"a", 2.0).validate(model).isInvalid)
  }

  test("invalid missing variable") {
    val model = Scalar(id"a", StringValueType)
    assert(MaxDelta(id"z", 2.0).validate(model).isInvalid)
  }

  test("invalid nested variable") {
    val model = ModelParser.unsafeParse("x -> y -> a")
    assert(MaxDelta(id"a", 2.0).validate(model).isInvalid)
  }

}

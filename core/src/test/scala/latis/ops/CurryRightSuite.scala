package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.data.Data.*
import latis.dsl.*

class CurryRightSuite extends CatsEffectSuite {

  private val model = ModelParser.unsafeParse("(x, y) -> a")
  private val data = DatasetGenerator.generateData(model)

  test("curry right model") {
    CurryRight(1).applyToModel(model).map { mod =>
      assertEquals(
        mod.toString,
        "x -> y -> a"
      )
    }.fold(e => fail(e.message), identity)
  }

  test("curry right samples") {
    CurryRight(1).pipe(model)(data.samples).flatMap {
      case Sample(DomainData(x: IntValue), RangeData(f: SampledFunction)) =>
        f.samples.map {
          case Sample(DomainData(y: IntValue), RangeData(a: IntValue)) =>
            (x.value, y.value, a.value)
          case _ => fail("Curried Sample is wrong")
        }
      case _ => fail("Curried Sample is wrong")
    }.compile.toList.map { obtained =>
      val expected = List(
        (0, 0, 0),
        (0, 1, 1),
        (0, 2, 2),
        (1, 0, 3),
        (1, 1, 4),
        (1, 2, 5)
      )
      assertEquals(obtained, expected)
    }
  }

  test("Curry right 2, model") {
    val model = ModelParser.unsafeParse("(x, y, z) -> a")
    CurryRight(2).applyToModel(model).map { mod =>
      assertEquals(
        mod.toString,
        "x -> (y, z) -> a"
      )
    }.fold(e => fail(e.message), identity)
  }

  test("Curry right 2, samples") {
    val model = ModelParser.unsafeParse("(x, y, z) -> a")
    val data = DatasetGenerator.generateData(model)
    CurryRight(2).pipe(model)(data.samples).flatMap {
      case Sample(DomainData(x: IntValue), RangeData(f: SampledFunction)) =>
        f.samples.map {
          case Sample(DomainData(y: IntValue, z: IntValue), RangeData(a: IntValue)) =>
            (x.value, y.value, z.value, a.value)
          case _ => fail("Curried Sample is wrong")
        }
      case _ => fail("Curried Sample is wrong")
    }.compile.toList.map { obtained =>
      val expected = List(
        (0,0,0,0),
        (0,0,1,1),
        (0,0,2,2),
        (0,0,3,3),
        (0,1,0,4),
        (0,1,1,5),
        (0,1,2,6),
        (0,1,3,7),
        (0,2,0,8),
        (0,2,1,9),
        (0,2,2,10),
        (0,2,3,11),
        (1,0,0,12),
        (1,0,1,13),
        (1,0,2,14),
        (1,0,3,15),
        (1,1,0,16),
        (1,1,1,17),
        (1,1,2,18),
        (1,1,3,19),
        (1,2,0,20),
        (1,2,1,21),
        (1,2,2,22),
        (1,2,3,23),
      )
      assertEquals(obtained, expected)
    }
  }
}

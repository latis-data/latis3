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

}

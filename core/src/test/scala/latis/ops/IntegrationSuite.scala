package latis.ops

import fs2.Stream
import munit.CatsEffectSuite

import latis.data.*
import latis.data.Data.*
import latis.dsl.*
import latis.util.Identifier.*

class IntegrationSuite extends CatsEffectSuite {

  test("integrate flat function, model") {
    val model = ModelParser.unsafeParse("x -> (a, b)")
    val obtained = SumIntegration(id"x").applyToModel(model)
      .fold(e => fail(e.message), identity)
    assertEquals(obtained.toString, "(a, b)")
  }

  test("integrate nested function, model") {
    val model = ModelParser.unsafeParse("x -> y -> (a, b)")
    val obtained = SumIntegration(id"y").applyToModel(model)
      .fold(e => fail(e.message), identity)
    assertEquals(obtained.toString, "x -> (a, b)")
  }

  test("integrate flat function, samples") {
    val model = ModelParser.unsafeParse("x -> (a, b)")
    val samples = List(
      Sample(DomainData(0), RangeData(0, 0)),
      Sample(DomainData(1), RangeData(1, 2)),
      Sample(DomainData(2), RangeData(2, 4))
    )
    val expected = RangeData(3L, 6L)

    SumIntegration(id"x").pipe(model)(Stream.emits(samples))
      .compile.toList.map { ss =>
        ss.head match {
          case Sample(DomainData(), rdata) =>
            assertEquals(rdata, expected)
        }
      }
  }

  test("integrate nested function, samples") {
    val model = ModelParser.unsafeParse("x -> y -> (a, b)")
    val f1 = SampledFunction(List(
      Sample(DomainData(0), RangeData(0, 0)),
      Sample(DomainData(1), RangeData(1, 2)),
      Sample(DomainData(2), RangeData(2, 4))
    ))
    val samples = List(
      Sample(DomainData(0), RangeData(f1))
    )
    val expected = RangeData(3L, 6L)
    SumIntegration(id"y").pipe(model)(Stream.emits(samples))
      .compile.toList.map { ss =>
        ss.head match {
          case Sample(DomainData(x: IntValue), rdata) =>
            assertEquals(x.value, 0)
            assertEquals(rdata, expected)
        }
      }
  }

}

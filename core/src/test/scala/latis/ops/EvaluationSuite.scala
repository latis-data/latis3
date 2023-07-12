package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dsl.*

class EvaluationSuite extends CatsEffectSuite {

  test("evaluate a 1D dataset") {
    DatasetGenerator.generate1DDataset(
      Vector(0, 1, 2),
      Vector(10, 20, 30)
    ).withOperation(Evaluation("1")).samples.head.map {
      case Sample(_, RangeData(Number(d))) =>
        assertEquals(d, 20.0)
      case _ => fail("unexpected sample")
    }.compile.drain
  }

  test("evaluate a nested dataset") {
    val ds = DatasetGenerator.generate2DDataset(
      Vector(0, 1, 2),
      Vector(100, 200, 300),
      Vector(
        Vector(10, 20, 30),
        Vector(12, 22, 32),
        Vector(14, 24, 34)
      )
    ).curry(1)
     .eval("1")

    ds.samples.take(1).map {
      case Sample(DomainData(Number(x)), RangeData(Number(a))) =>
        assertEquals(x, 100.0)
        assertEquals(a, 12.0)
      case _ => fail("unexpected sample")
    }.compile.drain
  }

  //TODO verify failure modes
}

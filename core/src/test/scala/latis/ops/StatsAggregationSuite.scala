package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dsl.DatasetGenerator

class StatsAggregationSuite extends CatsEffectSuite {

  test("statistics aggregation") {
    DatasetGenerator("x -> a").withOperation(StatsAggregation())
      .samples.compile.toList.map {
        case Sample(_, RangeData(Number(mean), Number(min), Number(max), Integer(count))) :: Nil =>
          assertEquals(mean, 1.0)
          assertEquals(min, 0.0)
          assertEquals(max, 2.0)
          assertEquals(count, 3L)
        case s => fail("Invalid stats result")
      }
  }

}

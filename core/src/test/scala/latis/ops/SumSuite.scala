package latis.ops

import munit.CatsEffectSuite

import latis.data._
import latis.dsl.DatasetGenerator

class SumSuite extends CatsEffectSuite {

  test("sum function with scalar range") {
    DatasetGenerator("x -> a: int") //makes a dataset with range values: 0, 1, 2
      .withOperation(Sum())
      .samples.compile.toList.map {
        // Single zero-arity Sample with integer sum data
        case Sample(DomainData(), RangeData(lv: Data.LongValue)) :: Nil =>
          assertEquals(lv.value, 3L)
        case _ => fail("unexpected samples")
      }
  }

  test("sum function with mixed tuple range") {
    DatasetGenerator("x -> (a: int, b: double, c: string)")
      .withOperation(Sum())
      .samples.compile.toList.map {
        // Single zero-arity Sample with sum data
        case Sample(DomainData(), RangeData(a: Data.LongValue, b: Data.DoubleValue, c: Data.DoubleValue)) :: Nil =>
          assertEquals(a.value, 3L)
          assertEquals(b.value, 3D)
          assert(c.value.isNaN)
        case _ => fail("unexpected samples")
      }
  }

}

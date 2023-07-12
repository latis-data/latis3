package latis.ops

import munit.CatsEffectSuite

import latis.data._
import latis.dataset._
import latis.dsl.DatasetGenerator
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier._

class CountAggregationSuite extends CatsEffectSuite {

  test("count function") {
    DatasetGenerator("x -> a") //makes a dataset with 3 samples
      .withOperation(CountAggregation())
      .samples.compile.toList.map {
        // Single zero-arity Sample with count data
        case Sample(DomainData(), RangeData(lv: Data.LongValue)) :: Nil =>
          assertEquals(lv.value, 3L)
        case _ => fail("unexpected samples")
      }
  }

  test("count scalar") {
    val ds = {
      val model = Scalar(id"x", IntValueType)
      val data = Data.IntValue(0)
      new TappedDataset(Metadata(id"test"), model, data)
    }

    ds.withOperation(CountAggregation())
      .samples.compile.toList.map {
        case Sample(DomainData(), RangeData(lv: Data.LongValue)) :: Nil =>
          assertEquals(lv.value, 1L)
        case _ => fail("unexpected samples")
      }
  }

}

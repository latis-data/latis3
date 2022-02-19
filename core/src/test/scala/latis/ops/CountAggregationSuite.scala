package latis.ops

import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside._

import latis.data._
import latis.dataset._
import latis.dsl.DatasetGenerator
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier.IdentifierStringContext

class CountAggregationSuite extends AnyFunSuite {

  test("count function") {
    DatasetGenerator("x -> a") //makes a dataset with 3 samples
      .withOperation(CountAggregation())
      .samples.compile.toList.map {
        inside(_) {
          // Single zero-arity Sample with count data
          case Sample(DomainData(), RangeData(lv: Data.LongValue)) :: Nil =>
            assertResult(3)(lv.value)
        }
      }.unsafeRunSync()
  }

  test("count scalar") {
    val ds = {
      val model = Scalar(id"x", IntValueType)
      val data = Data.IntValue(0)
      new TappedDataset(Metadata(id"test"), model, data)
    }
    ds.withOperation(CountAggregation())
      .samples.compile.toList.map {
        inside(_) {
          // Single zero-arity Sample with count data
          case Sample(DomainData(), RangeData(lv: Data.LongValue)) :: Nil =>
            assertResult(1)(lv.value)
        }
      }.unsafeRunSync()
  }

}

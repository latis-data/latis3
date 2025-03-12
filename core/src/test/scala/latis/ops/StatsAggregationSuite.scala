package latis.ops

import munit.CatsEffectSuite

import latis.data.*
import latis.dataset.*
import latis.dsl.DatasetGenerator
import latis.metadata.Metadata
import latis.model.*
import latis.util.Identifier.*

class StatsAggregationSuite extends CatsEffectSuite {

  //test("group by bin with stats") {
  //  ds.withOperation(Projection(id"x", id"b"))
  //    .withOperation(GroupByBinWidth(3.0, StatsAggregation()).fold(e => fail(e.getMessage), identity))
  //    .samples.compile.toList.map { _.head match {
  //      case Sample(DomainData(Number(x)), RangeData(Number(mean), _*)) =>
  //        assertEquals(x, 0.0)
  //        assertEquals(mean, 14.0 / 3.0)
  //    }}
  //}

}

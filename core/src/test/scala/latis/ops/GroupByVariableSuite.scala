package latis.ops

import munit.FunSuite

import latis.data._
import latis.dataset._
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.util.Identifier._

class GroupByVariableSuite extends FunSuite {

  // (x, y) -> a
  private lazy val model = ModelParser.unsafeParse("(x, y) -> a")

  private lazy val data = SeqFunction(Seq(
    Sample(DomainData(0, 10), RangeData(1)),
    Sample(DomainData(0, 11), RangeData(2)),
    Sample(DomainData(1, 10), RangeData(3)),
    Sample(DomainData(1, 11), RangeData(4)),
  ))

  private lazy val ds = new MemoizedDataset(Metadata(id"test"), model, data)
      .withOperation(new GroupByVariable(id"y"))

  test("unProject the grouped variables") {
    assertEquals(ds.model.toString, "y -> x -> a")
  }
}

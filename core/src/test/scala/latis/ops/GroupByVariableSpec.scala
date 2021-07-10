package latis.ops

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.dataset._
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.util.Identifier.IdentifierStringContext

class GroupByVariableSpec extends AnyFlatSpec {

  // (x, y) -> a
  private val model = ModelParser.unsafeParse("(x, y) -> a")

  private val data = SeqFunction(Seq(
    Sample(DomainData(0, 10), RangeData(1)),
    Sample(DomainData(0, 11), RangeData(2)),
    Sample(DomainData(1, 10), RangeData(3)),
    Sample(DomainData(1, 11), RangeData(4)),
  ))

  private val ds = new MemoizedDataset(Metadata(id"test"), model, data)
      .withOperation(GroupByVariable(id"y"))
      .unsafeForce()

  "GroupByVariable" should "unProject the grouped variables" in {
    ds.model.toString should be ("y -> x -> a")
  }
}

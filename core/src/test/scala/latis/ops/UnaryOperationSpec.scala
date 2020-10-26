package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.ops.UnaryOperation._
import latis.util.Identifier.IdentifierStringContext

class UnaryOperationSpec extends FlatSpec {

  "makeOperation" should "make unary operations" in {
    makeOperation("curry", List()) should be (Right(Curry()))
    makeOperation("curry", List("2")) should be (Right(Curry(2)))
    makeOperation("evaluation", List("1")) should be (Right(Evaluation("1")))
    makeOperation("pivot", List("(1,2)", "(Fe,Mg)")) should be (Right(Pivot(Seq("1", "2"), Seq("Fe", "Mg"))))
    makeOperation("project", List("a", "b")) should be (Right(Projection(id"a", id"b")))
    makeOperation("rename", List("a", "b")) should be (Right(Rename(id"a", id"b")))
    makeOperation("timeTupleToTime", List()) should be (Right(TimeTupleToTime()))
    makeOperation("timeTupleToTime", List("Time")) should be (Right(TimeTupleToTime(id"Time")))
  }
}

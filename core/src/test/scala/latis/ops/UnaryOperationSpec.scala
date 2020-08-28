package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.ops.UnaryOperation._

class UnaryOperationSpec extends FlatSpec {

  "makeOperation" should "make unary operations" in {
    makeOperation("project", List("a", "b")) should be (Right(Projection("a", "b")))
    makeOperation("rename", List("a", "b")) should be (Right(Rename("a", "b")))
    makeOperation("curry", List("2")) should be (Right(Curry(2)))
    makeOperation("curry", List()) should be (Right(Curry()))
    makeOperation("pivot", List("(1,2)", "(Fe,Mg)")) should be (Right(Pivot(Seq("1", "2"), Seq("Fe", "Mg"))))
    makeOperation("evaluation", List("1")) should be (Right(Evaluation("1")))
  }
}

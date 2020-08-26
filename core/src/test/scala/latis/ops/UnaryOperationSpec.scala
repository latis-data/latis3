package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Inside._
import org.scalatest.Matchers._

import latis.data.Data
import latis.data.TupleData
import latis.ops.UnaryOperation._

class UnaryOperationSpec extends FlatSpec {

  "makeOperation" should "make unary operations" in {
    makeOperation("project", List("a", "b")) should be (Some(Projection("a", "b")))
    makeOperation("rename", List("a", "b")) should be (Some(Rename("a", "b")))
    makeOperation("curry", List("2")) should be (Some(Curry(2)))
    makeOperation("curry", List()) should be (Some(Curry()))
    makeOperation("pivot", List("(1,2)", "(Fe,Mg)")) should be (Some(Pivot(Seq("1", "2"), Seq("Fe", "Mg"))))
    makeOperation("evaluation", List("1")) should be (Some(Evaluation(Data.IntValue(1))))
    inside(makeOperation("evaluation", List("1", "b"))) { case Some(Evaluation(t)) =>
      t.asInstanceOf[TupleData].elements should be (Seq(Data.IntValue(1), Data.StringValue("b")))
    }
  }

}

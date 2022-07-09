package latis.ops

import munit.FunSuite

import latis.ops.UnaryOperation._
import latis.util.Identifier._

class UnaryOperationSuite extends FunSuite {

  test("make unary operations") {
    assertEquals(makeOperation("curry", List()), Right(Curry()))

    assertEquals(makeOperation("curry", List("2")), Right(Curry(2)))

    assertEquals(
      makeOperation("evaluation", List("1")),
      Right(Evaluation("1"))
    )

    assertEquals(
      makeOperation("pivot", List("(1,2)", "(Fe,Mg)")),
      Right(Pivot(Seq("1", "2"), Seq("Fe", "Mg")))
    )

    assertEquals(
      makeOperation("project", List("a", "b")),
      Right(Projection(id"a", id"b"))
    )

    assertEquals(
      makeOperation("rename", List("a", "b")),
      Right(Rename(id"a", id"b"))
    )

    assertEquals(
      makeOperation("timeTupleToTime", List()),
      Right(TimeTupleToTime())
    )

    assertEquals(
      makeOperation("timeTupleToTime", List("Time")),
      Right(TimeTupleToTime(id"Time"))
    )
  }
}

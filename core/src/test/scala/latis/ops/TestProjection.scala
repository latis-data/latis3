package latis.ops

import org.junit.Assert._
import org.junit.Ignore
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import latis.model._

class TestProjection {
  
  @Test
  def tuple() = {
    val model = Tuple(Scalar("a"), Scalar("b"), Scalar("c"))
    val proj = Projection("a", "b")
    proj.applyToModel(model) match {
      case Tuple(a: Scalar, b: Scalar) =>
        assertEquals("a", a.id)
        assertEquals("b", b.id)
    }
  }
}
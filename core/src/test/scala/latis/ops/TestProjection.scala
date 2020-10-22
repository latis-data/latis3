package latis.ops

import org.junit.Assert._
import org.junit.Ignore
import org.junit.Test
import org.scalatestplus.junit.JUnitSuite

import latis.metadata.Metadata
import latis.model._

class TestProjection {
  
  @Test
  def tuple() = {
    val model = Tuple(
      Scalar(Metadata("id" -> "a", "type" -> "int")),
      Scalar(Metadata("id" -> "b", "type" -> "int")),
      Scalar(Metadata("id" -> "c", "type" -> "int"))
    )
    val proj = Projection("a", "b")
    proj.applyToModel(model) match {
      case Right(Tuple(a: Scalar, b: Scalar)) =>
        assertEquals("a", a.id)
        assertEquals("b", b.id)
    }
  }
}

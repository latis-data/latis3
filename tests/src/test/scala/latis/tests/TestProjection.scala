package latis.tests

import org.junit.Assert._
import org.junit.Test

import latis.metadata.Metadata
import latis.model._
import latis.ops.Projection
import latis.util.Identifier.IdentifierStringContext

class TestProjection {
  
  @Test
  def tuple() = {
    val model = Tuple(
      Scalar(Metadata("id" -> "a", "type" -> "int")),
      Scalar(Metadata("id" -> "b", "type" -> "int")),
      Scalar(Metadata("id" -> "c", "type" -> "int"))
    )
    val proj = Projection(id"a", id"b")
    proj.applyToModel(model) match {
      case Right(Tuple(a: Scalar, b: Scalar)) =>
        assertEquals("a", a.id)
        assertEquals("b", b.id)
    }
  }
}

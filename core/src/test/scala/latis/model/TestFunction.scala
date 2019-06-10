package latis.model

import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import latis.metadata.Metadata

class TestFunction extends JUnitSuite {
  
  @Test
  def extract = {
    val f = Function(Metadata("id" -> "f"), Scalar("a"), Scalar("b"))
    f match {
      case Function(d,r) => println(s"$d -> $r")
    }

  }
}

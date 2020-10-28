package latis.model

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import atto._
import Atto._

import latis.util.LatisException

class ModelParserSpec extends FlatSpec {

  "The ModelParser" should "parse a scalar" in {
     ModelParser.parse("foo") match {
      case Right(s: Scalar) => s.id.get.asString should be ("foo")
    }
  }

  it should "parse a tuple" in {
    ModelParser.parse("(a, b)") match {
      case Right(Tuple(a: Scalar, b: Scalar)) =>
        a.id.get.asString should be ("a")
        b.id.get.asString should be ("b")
    }
  }

  it should "parse a function" in {
    ModelParser.parse("x -> a") match {
      case Right(Function(x: Scalar, a: Scalar)) =>
        x.id.get.asString should be ("x")
        a.id.get.asString should be ("a")
    }
  }

  it should "parse a complex function" in {
    ModelParser.parse("(x, y) -> (a, b)") match {
      case Right(f) => f.toString should be ("(x, y) -> (a, b)")
    }
  }

  it should "parse a nested function" in {
    ModelParser.parse("(x, y) -> w -> (a, b)") match {
      case Right(f) => f.toString should be ("(x, y) -> w -> (a, b)")
    }
  }

  it should "make an exception for an invalid model expression" in {
    ModelParser.parse("a <- b") match {
      case Left(le: LatisException) =>
        le.message.take(6) should be ("Failed")
    }
  }
}

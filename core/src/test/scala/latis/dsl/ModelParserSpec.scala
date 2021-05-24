package latis.dsl

import atto._
import atto.Atto._
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.metadata.Metadata
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.model.Tuple
import latis.util.Identifier.IdentifierStringContext
import latis.util.LatisException


class ModelParserSpec extends AnyFlatSpec {

  private val testScalar   = testParser(ModelParser.scalar)(_, _)
  private val testTuple    = testParser(ModelParser.tuple)(_, _)
  private val testFunction = testParser(ModelParser.function)(_, _)

  "The ModelParser" should "parse a scalar" in {
    testScalar("b", Scalar(Metadata(id"b") + ("type" -> "int")))
  }

  it should "parse a scalar with a type" in {
    testScalar("a: double", Scalar(Metadata(id"a") + ("type" -> "double")))
  }

  it should "parse a tuple" in {
    testTuple(
      "(a: double, b: int)",
      Tuple(
        Scalar(Metadata(id"a") + ("type" -> "double")),
        Scalar(Metadata(id"b") + ("type" -> "int"))
      )
    )
  }

  it should "parse a tuple of more than two elements" in {
    testTuple(
      "(a, b, c)",
      Tuple(
        Scalar(Metadata(id"a") + ("type" -> "int")),
        Scalar(Metadata(id"b") + ("type" -> "int")),
        Scalar(Metadata(id"c") + ("type" -> "int"))
      )
    )
  }

  it should "parse a nested tuple" in {
    ModelParser.unsafeParse("(a, (b, c))") match {
      case Tuple(a: Scalar, Tuple(b: Scalar, c: Scalar)) =>
        a.id.get should be (id"a")
        b.id.get should be (id"b")
        c.id.get should be (id"c")
    }
  }

  ignore should "parse a function in a tuple" in {
    ModelParser.unsafeParse("(a, b -> c)") match {
      case Tuple(a: Scalar, Function(b: Scalar, c: Scalar)) =>
        a.id.get should be (id"a")
        b.id.get should be (id"b")
        c.id.get should be (id"c")
    }
  }

  it should "parse a function" in {
    testFunction(
      "a: int -> b: double",
      Function(
        Scalar(Metadata(id"a") + ("type" -> "int")),
        Scalar(Metadata(id"b") + ("type" -> "double"))
      )
    )
  }

  it should "parse a complex function" in {
    testFunction(
      "(a: string, b: int) -> (c: double, d: double)",
      Function(
        Tuple(
          Scalar(Metadata(id"a") + ("type" -> "string")),
          Scalar(Metadata(id"b") + ("type" -> "int"))
        ),
        Tuple(
          Scalar(Metadata(id"c") + ("type" -> "double")),
          Scalar(Metadata(id"d") + ("type" -> "double"))
        )
      )
    )
  }

  it should "parse a nested function" in {
    testFunction(
      "a: int -> b: double -> c: double",
      Function(
        Scalar(Metadata(id"a") + ("type" -> "int")),
        Function(
          Scalar(Metadata(id"b") + ("type" -> "double")),
          Scalar(Metadata(id"c") + ("type" -> "double"))
        )
      )
    )
  }

  it should "parse a nested function with parentheses" in {
    testFunction(
      "a: int -> (b: double -> c: double)",
      Function(
        Scalar(Metadata(id"a") + ("type" -> "int")),
        Function(
          Scalar(Metadata(id"b") + ("type" -> "double")),
          Scalar(Metadata(id"c") + ("type" -> "double"))
        )
      )
    )
  }

  it should "make an exception for an invalid model expression" in {
    ModelParser.parse("a <- b") match {
      case Left(le: LatisException) =>
        le.message.take(6) should be("Failed")
    }
  }

  /**
   * Applies custom equality to DataType for testing
   * TODO: move to package object of type class instances for testing
   */
  implicit val modelEq: Equality[DataType] = new Equality[DataType] {
    override def areEqual(model: DataType, expected: Any): Boolean =
      (model, expected) match {
        case (m: Scalar, e: Scalar) =>
          (m.id == e.id) && (m.valueType == e.valueType)
        case (m: Tuple, e: Tuple) =>
          (m.id == e.id) && m.elements.zip(e.elements).forall(t => areEqual(t._1, t._2))
        case (m: Function, e: Function) =>
          (m.id == e.id) && areEqual(m.domain, e.domain) && areEqual(m.range, e.range)
        case _ => false
      }
  }

  /**
   * Partially apply with a parser to get a function that takes the string you
   * want to parse and the thing you expect to get back
   */
  private def testParser[A](p: Parser[A])(s: String, d: A): Unit = p.parseOnly(s) match {
    case ParseResult.Done(_, result: DataType) => result should equal(d)
    case ParseResult.Done(_, result)           => result should be(d)
    case ParseResult.Fail(_, _, m)             => fail(s"$m in $s")
    // parseOnly will never return anything but Done or Fail, but the types don't
    // know that so we get a warning without the following line.
    case _ => fail(s"failed to parse $s")
  }
}

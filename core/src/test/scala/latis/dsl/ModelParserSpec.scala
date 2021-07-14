package latis.dsl

import atto._
import atto.Atto._
import org.scalactic.Equality
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Assertion
import org.scalatest.Inside.inside

import latis.model._
import latis.util.Identifier.IdentifierStringContext
import latis.util.LatisException


class ModelParserSpec extends AnyFlatSpec {

  private lazy val testScalar   = testParser(ModelParser.scalar)(_, _)
  private lazy val testTuple    = testParser(ModelParser.tuple)(_, _)
  private lazy val testFunction = testParser(ModelParser.function)(_, _)

  private lazy val a = Scalar(id"a", DoubleValueType)
  private lazy val b = Scalar(id"b", IntValueType)
  private lazy val c = Scalar(id"c", StringValueType)
  private lazy val d = Scalar(id"d", LongValueType)

  "The ModelParser" should "parse a scalar" in {
    testScalar("b", b)
  }

  it should "parse a scalar with a type" in {
    testScalar("a: double", a)
  }

  it should "parse a tuple" in {
    testTuple(
      "(a: double, b: int)",
      Tuple.fromElements(a, b).value
    )
  }

  it should "parse a tuple of more than two elements" in {
    testTuple(
      "(a: double, b, c: string)",
      Tuple.fromElements(a, b, c).value
    )
  }

  it should "parse a nested tuple" in {
    inside(ModelParser.unsafeParse("(a, (b, c))")) {
      case Tuple(a: Scalar, Tuple(b: Scalar, c: Scalar)) =>
        a.id should be (id"a")
        b.id should be (id"b")
        c.id should be (id"c")
    }
  }

  ignore should "parse a function in a tuple" in {
    inside(ModelParser.unsafeParse("(a, b -> c)")) {
      case Tuple(a: Scalar, Function(b: Scalar, c: Scalar)) =>
        a.id should be (id"a")
        b.id should be (id"b")
        c.id should be (id"c")
    }
  }

  it should "parse a function" in {
    testFunction(
      "b: int -> a: double",
      Function.from(b, a).value
    )
  }

  it should "parse a complex function" in {
    val f = for {
      domain <- Tuple.fromElements(c, b)
      range <- Tuple.fromElements(a, d)
      f <- Function.from(domain, range)
    } yield f
    testFunction(
      "(c: string, b: int) -> (a: double, d: long)",
      f.value
    )
  }

  it should "parse a nested function" in {
    val inner = Function.from(b, c).value
    val outer = Function.from(a, inner).value
    testFunction(
      "a: double -> b: int -> c: string",
      outer
    )
  }

  it should "parse a nested function with parentheses" in {
    val inner = Function.from(b, c).value
    val outer = Function.from(a, inner).value
    testFunction(
      "a: double -> (b: int -> c: string)",
      outer
    )
  }

  it should "make an exception for an invalid model expression" in {
    inside(ModelParser.parse("a <- b")) {
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
  private def testParser[A](p: Parser[A])(s: String, d: A): Assertion = p.parseOnly(s) match {
    case ParseResult.Done(_, result: DataType) => result should equal(d)
    case ParseResult.Done(_, result)           => result should be(d)
    case ParseResult.Fail(_, _, m)             => fail(s"$m in $s")
    // parseOnly will never return anything but Done or Fail, but the types don't
    // know that so we get a warning without the following line.
    case _ => fail(s"failed to parse $s")
  }
}

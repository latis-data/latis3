package latis.dsl

import atto.Atto._
import atto._
import cats.Eq
import cats.syntax.all._
import munit.FunSuite

import latis.model._
import latis.util.Identifier._
import latis.util.LatisException

class ModelParserSuite extends FunSuite {

  private lazy val testScalar   = testParser(ModelParser.scalar)(_, _)
  private lazy val testTuple    = testParser(ModelParser.tuple)(_, _)
  private lazy val testFunction = testParser(ModelParser.function)(_, _)

  private lazy val a = Scalar(id"a", DoubleValueType)
  private lazy val b = Scalar(id"b", IntValueType)
  private lazy val c = Scalar(id"c", StringValueType)
  private lazy val d = Scalar(id"d", LongValueType)

  test("parse a scalar") {
    testScalar("b", b)
  }

  test("parse a scalar with a type") {
    testScalar("a: double", a)
  }

  test("parse a tuple") {
    testTuple(
      "(a: double, b: int)",
      Tuple.fromElements(a, b).getOrElse(fail("tuple not generated"))
    )
  }

  test("parse a tuple of more than two elements") {
    testTuple(
      "(a: double, b, c: string)",
      Tuple.fromElements(a, b, c).getOrElse(fail("tuple not generated"))
    )
  }

  test("parse a nested tuple") {
    ModelParser.unsafeParse("(a, (b, c))") match {
      case Tuple(a: Scalar, Tuple(b: Scalar, c: Scalar)) =>
        assertEquals(a.id, id"a")
        assertEquals(b.id, id"b")
        assertEquals(c.id, id"c")
      case _ => fail("model is not tuple of correct type")
    }
  }

  test("parse a function in a tuple".ignore) {
    ModelParser.unsafeParse("(a, b -> c)") match {
      case Tuple(a: Scalar, Function(b: Scalar, c: Scalar)) =>
        assertEquals(a.id, id"a")
        assertEquals(b.id, id"b")
        assertEquals(c.id, id"c")
      case _ => fail("model is not tuple of correct type")
    }
  }

  test("parse a function") {
    testFunction(
      "b: int -> a: double",
      Function.from(b, a).getOrElse(fail("function not generated"))
    )
  }

  test("parse a complex function") {
    val f = for {
      domain <- Tuple.fromElements(c, b)
      range <- Tuple.fromElements(a, d)
      f <- Function.from(domain, range)
    } yield f
    testFunction(
      "(c: string, b: int) -> (a: double, d: long)",
      f.getOrElse(fail("function not generated"))
    )
  }

  test("parse a nested function") {
    val inner = Function.from(b, c).getOrElse(fail("function not generated"))
    val outer = Function.from(a, inner).getOrElse(fail("function not generated"))
    testFunction(
      "a: double -> b: int -> c: string",
      outer
    )
  }

  test("parse a nested function with parentheses") {
    val inner = Function.from(b, c).getOrElse(fail("function not generated"))
    val outer = Function.from(a, inner).getOrElse(fail("function not generated"))
    testFunction(
      "a: double -> (b: int -> c: string)",
      outer
    )
  }

  test("make an exception for an invalid model expression") {
    ModelParser.parse("a <- b") match {
      case Left(le: LatisException) =>
        assertEquals(le.message.take(6), "Failed")
      case _ => fail("Model parsed incorrectly")
    }
  }

  /**
   * Applies custom equality to DataType for testing
   */
  implicit val modelEq: Eq[DataType] = new Eq[DataType] {
    override def eqv(obtained: DataType, expected: DataType): Boolean =
      (obtained, expected) match {
        case (m: Scalar, e: Scalar) =>
          (m.id == e.id) && (m.valueType == e.valueType)
        case (m: Tuple, e: Tuple) =>
          (m.id == e.id) && m.elements.zip(e.elements).forall(t => eqv(t._1, t._2))
        case (m: Function, e: Function) =>
          (m.id == e.id) && eqv(m.domain, e.domain) && eqv(m.range, e.range)
        case _ => false
      }
  }

  /**
   * Partially apply with a parser to get a function that takes the string you
   * want to parse and the thing you expect to get back
   */
  private def testParser[A: Eq](p: Parser[A])(s: String, d: A): Unit = p.parseOnly(s) match {
    case ParseResult.Done(_, result) => assert(result === d)
    case ParseResult.Fail(_, _, m)   => fail(s"$m in $s")
    // parseOnly will never return anything but Done or Fail, but the types don't
    // know that so we get a warning without the following line.
    case _ => fail(s"failed to parse $s")
  }
}

package latis.util.dap2.parser

import org.scalacheck._
import org.scalacheck.ScalacheckShapeless._

import latis.ops.parser.ast._
import ast._
import latis.util.Identifier

object ConstraintParserProps extends Properties("DAP 2 Constraint Parser") {

  property("parse") = Prop.forAll(cexpr) { expr: ConstraintExpression =>
    expr == ConstraintParser.parse(pretty(expr)).fold(
      msg => throw new RuntimeException(msg), x => x
    )
  }

  val identifier: Gen[Identifier] = for {
    init <- Gen.oneOf(Gen.alphaChar, Gen.const('_'))
    //TODO: remove '.' after Identifier refactor
    rest <- Gen.listOf(Gen.oneOf(Gen.alphaNumChar, Gen.const('_'), Gen.const('.')))
  } yield Identifier.fromString(init + rest.mkString).getOrElse( ??? ) //TODO: not this getOrElse

  val variable: Gen[String] = for {
    init <- identifier
    rest <- Gen.oneOf(Gen.const(""), identifier.map("." + _))
  } yield init + rest

  val operator: Gen[SelectionOp] =
    implicitly[Arbitrary[SelectionOp]].arbitrary

  val sign: Gen[String] = Gen.oneOf("+", "-", "")

  val integer: Gen[String] = for {
    s <- sign
    n <- Gen.numStr.retryUntil(_.length > 0)
  } yield s + n

  val decimal: Gen[String] = {
    // A decimal variant for which the fractional part is optional.
    val n1: Gen[String] = for {
      int  <- Gen.numStr.retryUntil(_.length > 0)
      frac <- Gen.oneOf(Gen.numStr, Gen.const(""))
    } yield s"$int.$frac"

    // A decimal variant for which the integral part is optional.
    val n2: Gen[String] = for {
      int  <- Gen.oneOf(Gen.numStr, Gen.const(""))
      frac <- Gen.numStr.retryUntil(_.length > 0)
    } yield s"$int.$frac"

    for {
      s <- sign
      n <- Gen.oneOf(n1, n2)
    } yield s + n
  }

  val scientific: Gen[String] = for {
    significand <- Gen.oneOf(decimal, integer)
    e           <- Gen.oneOf("e", "E")
    exponent    <- integer
  } yield significand + e + exponent

  val number: Gen[String] =
    Gen.oneOf(integer, decimal, scientific)

  val time: Gen[String] = {
    def stringN(n: Int, g: Gen[Char]): Gen[String] =
      Gen.listOfN(n, g).map(_.mkString)

    val timeG: Gen[String] = for {
      h <- stringN(2, Gen.numChar)
      m <- stringN(2, Gen.numChar)
      s <- stringN(2, Gen.numChar)
    } yield s"T$h:$m:$s"

    for {
      y <- stringN(4, Gen.numChar)
      m <- stringN(2, Gen.numChar)
      d <- stringN(2, Gen.numChar)
      t <- Gen.oneOf(timeG, Gen.const(""))
    } yield s"$y-$m-$d$t"
  }

  val stringLit: Gen[String] =
    Gen.alphaNumStr.retryUntil(_.length > 0).map("\"" + _ + "\"")

  val projection: Gen[CExpr] =
    //TODO: maybe revert to Gen.listOf(variable) after Identifier refactor
    Gen.listOf(identifier).retryUntil(_.length > 0).map(Projection)

  val selection: Gen[CExpr] = for {
    n <- identifier
    o <- operator
    v <- Gen.oneOf(time, number, stringLit)
  } yield Selection(n, o, v)

  val operation: Gen[CExpr] = for {
    n <- identifier
    a <- Gen.listOf(Gen.oneOf(time, number, stringLit, variable))
  } yield Operation(n.asString, a)

  val expr: Gen[CExpr] = Gen.oneOf(projection, selection, operation)

  val cexpr: Gen[ConstraintExpression] =
    Gen.listOf(expr).map(ConstraintExpression)
}

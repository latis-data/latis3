package latis.util.dap2.parser

import org.scalacheck._

import latis.util.Identifier

import ast._

object ConstraintParserProps extends Properties("DAP 2 Constraint Parser") {

  property("parse") = Prop.forAll(cexpr) { expr: ConstraintExpression =>
    expr == ConstraintParser.parse(pretty(expr)).fold(
      msg => throw new RuntimeException(msg), x => x
    )
  }

  val identifier: Gen[Identifier] = for {
    init <- Gen.oneOf(Gen.alphaChar, Gen.const('_'))
    //TODO: remove const('.') after Identifier refactor
    rest <- Gen.listOf(Gen.oneOf(Gen.alphaNumChar, Gen.const('_'), Gen.const('.')))
    id    = s"$init${rest.mkString}"
  } yield Identifier.fromString(id).getOrElse {
    throw new RuntimeException(s"Failed to generate identifier from: $id")
  }

  val variable: Gen[String] = for {
    init <- identifier
    rest <- Gen.oneOf(Gen.const(""), identifier.map("." + _.asString))
  } yield init.asString + rest

  val operator: Gen[SelectionOp] =
    Gen.oneOf(Gt, Lt, Eq, GtEq, LtEq, EqEq, NeEq, Tilde, EqTilde, NeEqTilde)

  val sign: Gen[String] = Gen.oneOf("+", "-", "")

  val integer: Gen[String] = for {
    s <- sign
    n <- Gen.nonEmptyListOf(Gen.numChar).map(_.mkString)
  } yield s + n

  val decimal: Gen[String] = {
    // A decimal variant for which the fractional part is optional.
    val n1: Gen[String] = for {
      int  <- Gen.nonEmptyListOf(Gen.numChar).map(_.mkString)
      frac <- Gen.oneOf(Gen.numStr, Gen.const(""))
    } yield s"$int.$frac"

    // A decimal variant for which the integral part is optional.
    val n2: Gen[String] = for {
      int  <- Gen.oneOf(Gen.numStr, Gen.const(""))
      frac <- Gen.nonEmptyListOf(Gen.numChar).map(_.mkString)
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
    Gen.nonEmptyListOf(Gen.alphaNumChar).map(x => s""""${x.mkString}"""")

  val projection: Gen[CExpr] =
    //TODO: maybe revert to Gen.listOf(variable) after Identifier refactor
    Gen.nonEmptyListOf(identifier).map(Projection)

  val selection: Gen[CExpr] = for {
    n <- identifier
    o <- operator
    v <- Gen.oneOf(time, number, stringLit)
  } yield Selection(n, o, v)

  val operation: Gen[CExpr] = for {
    n <- identifier
    a <- Gen.listOf(Gen.oneOf(time, number, stringLit, variable))
  } yield Operation(n.asString, a)

  val cexpr: Gen[ConstraintExpression] = for {
    first <- Gen.option(Gen.oneOf(projection, selection, operation))
    rest  <- Gen.listOf(Gen.oneOf(selection, operation))
  } yield ConstraintExpression(first.toList ++ rest)
}

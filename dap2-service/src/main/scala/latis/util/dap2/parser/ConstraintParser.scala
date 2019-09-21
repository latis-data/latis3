package latis.util.dap2.parser

import atto._, Atto._
import cats.implicits._

import ast._

/**
 * Module for parsing DAP 2 constraint expressions.
 *
 * This replicates what was done in LaTiS 2 rather than strictly
 * follow the DAP 2 specification.
 */
object ConstraintParser {

  /**
   * Parse a DAP 2 constraint expression into a sequence of LaTiS
   * operations.
   *
   * @param expr non-URL-encoded DAP 2 constraint expression
   */
  def parse(expr: String): Either[String, ast.ConstraintExpression] = {
    def p(sexpr: String): Either[String, ast.CExpr] =
      (phrase(subexpression) | err(s"Invalid subexpression: $sexpr"))
        .parseOnly(sexpr).either

    if (expr.nonEmpty) {
      val sexprs = expr.dropWhile(_ === '&').split("&").toList
      sexprs.traverse(p).map(ast.ConstraintExpression)
    } else {
      Right(ast.ConstraintExpression(List.empty))
    }
  }

  /*
   * A note about "|" and "choice":
   *
   * The order of parsers combined by "|" or "choice" is important.
   * The resulting parser will stop parsing after the first success.
   *
   * For instance, imagine we want to parse one of ">" or ">=". If ">"
   * appears first, the parser will never reach ">=" because ">" will
   * succeed.
   */

  private def subexpression: Parser[CExpr] =
    selection | operation | projection

  private def projection: Parser[CExpr] =
    sepBy1(variable.token, char(',').token).map(xs => Projection(xs.toList))

  private def selectionOp: Parser[SelectionOp] = choice(
    string("~")   ~> ok(Tilde),
    string(">=")  ~> ok(GtEq),
    string("<=")  ~> ok(LtEq),
    string("==")  ~> ok(EqEq),
    string("=~")  ~> ok(EqTilde),
    string("!=~") ~> ok(NeEqTilde),
    string("!=")  ~> ok(NeEq),
    string(">")   ~> ok(Gt),
    string("<")   ~> ok(Lt),
    string("=")   ~> ok(Eq)
  )

  private def selection: Parser[CExpr] = for {
    name  <- variable.token
    op    <- selectionOp.token
    value <- time | number | stringLit
  } yield Selection(name, op, value)

  private def operation: Parser[CExpr] = for {
    name <- identifier
    argP  = time | number | stringLit | variable
    args <- parens(sepBy(argP.token, char(',').token))
  } yield Operation(name, args)

  private def variable: Parser[String] =
    sepBy1(identifier, char('.')).map(_.toList.mkString("."))

  private def identifier: Parser[String] = for {
    init <- letter | char('_')
    rest <- many(letterOrDigit | char('_'))
  } yield (init :: rest).mkString

  private def stringLit: Parser[String] = for {
    lit <- stringLiteral
  } yield "\"" + lit + "\""

  private def sign: Parser[String] = string("+") | string("-")

  private def integer: Parser[String] = for {
    s <- sign | ok("")
    n <- stringOf1(digit)
  } yield s + n

  private def decimal: Parser[String] = {
    // A decimal variant for which the fractional part is optional.
    val n1: Parser[String] = for {
      int  <- stringOf1(digit)
      dec  <- string(".")
      frac <- stringOf(digit)
    } yield int + dec + frac

    // A decimal variant for which the integral part is optional.
    val n2: Parser[String] = for {
      int  <- stringOf(digit)
      dec  <- string(".")
      frac <- stringOf1(digit)
    } yield int + dec + frac

    for {
      s <- sign | ok("")
      n <- n1 | n2
    } yield s + n
  }

  private def scientific: Parser[String] = for {
    significand <- decimal | integer
    e           <- string("e") | string("E")
    exponent    <- integer
  } yield significand + e + exponent

  private def number: Parser[String] = scientific | decimal | integer

  private def time: Parser[String] = {
    def stringN(n: Int, p: Parser[Char]): Parser[String] =
      count(n, p).map(_.mkString)

    val timeP: Parser[String] = for {
      _ <- char('T')
      h <- stringN(2, digit) <~ char(':')
      m <- stringN(2, digit) <~ char(':')
      s <- stringN(2, digit)
    } yield "T" + List(h, m, s).mkString(":")

    for {
      y <- stringN(4, digit) <~ char('-')
      m <- stringN(2, digit) <~ char('-')
      d <- stringN(2, digit)
      t <- timeP | ok("")
    } yield List(y, m, d).mkString("-") + t
  }
}

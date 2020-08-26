package latis.ops.parser

import atto.Atto._
import atto.Parser

import latis.data.Data
import latis.ops.parser.ast._

object parsers {

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

  def subexpression: Parser[CExpr] =
    selection | operation | projection

  def projection: Parser[CExpr] =
    sepBy1(variable.token, char(',').token).map(xs => Projection(xs.toList))

  def selectionOp: Parser[SelectionOp] = choice(
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

  def selection: Parser[CExpr] = for {
    name  <- variable.token
    op    <- selectionOp.token
    value <- time | number | stringLit
  } yield Selection(name, op, value)

  def operation: Parser[CExpr] = for {
    name <- identifier
    argsP <- parens(args)
  } yield Operation(name, argsP)

  def args: Parser[List[String]] = sepBy(arg.token, char(',').token)

  def arg: Parser[String] =
    scalarArg | listArg

  def scalarArg: Parser[String] =
    time | number | stringLit | variable

  def listArg: Parser[String] = for {
    l <- parens(sepBy(arg.token, char(',').token))
  } yield "(" + l.mkString(",") + ")"

  def variable: Parser[String] =
    sepBy1(identifier, char('.')).map(_.toList.mkString("."))

  def identifier: Parser[String] = for {
    init <- letter | char('_')
    rest <- many(letterOrDigit | char('_'))
  } yield (init :: rest).mkString

  def stringLit: Parser[String] = for {
    lit <- stringLiteral
  } yield "\"" + lit + "\""

  def sign: Parser[String] = string("+") | string("-")

  def integer: Parser[String] = for {
    s <- sign | ok("")
    n <- stringOf1(digit)
  } yield s + n

  def decimal: Parser[String] = {
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

  def scientific: Parser[String] = for {
    significand <- decimal | integer
    e           <- string("e") | string("E")
    exponent    <- integer
  } yield significand + e + exponent

  def number: Parser[String] = scientific | decimal | integer

  def time: Parser[String] = {
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

  def booleanValue: Parser[Data] = for {
    n <- string("true") | string("True") | string("false") | string("False")
  } yield Data.BooleanValue(n.toBoolean)

  def floatValue: Parser[Data] = for {
    n <- decimal
    _ <- char('f') | char('F')
  } yield Data.FloatValue(n.toFloat)

  def doubleValue: Parser[Data] = for {
    n <- scientific | decimal
  } yield Data.DoubleValue(n.toDouble)

  def intValue: Parser[Data] = for {
    n <- integer
  } yield Data.IntValue(n.toInt)

  def longValue: Parser[Data] = for {
    n <- integer
    _ <- char('l') | char('L')
  } yield Data.LongValue(n.toLong)

  def numberValue: Parser[Data] =
    floatValue | doubleValue | longValue | intValue

  def stringValue: Parser[Data] = for {
    s <- variable.token
  // TODO: this will stop once it encounters a character that is not allowed as
  //   a variable. Do we want to make StringValues from any string?
  } yield Data.StringValue(s)

  def data: Parser[Data] =
    numberValue | booleanValue | stringValue
}

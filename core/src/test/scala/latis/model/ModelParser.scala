package latis.model

import atto.Atto._
import atto._
import cats.syntax.all._

import latis.metadata.Metadata
import latis.util.LatisException
import latis.util.dap2.parser.parsers.identifier

/**
 * Defines a parser that turns a functional data model expression
 * into a DataType.
 */
object ModelParser {
  private val defaultScalarType: String = "Int"

  def parse(exp: String): Either[LatisException, DataType] =
    phrase(dataType).parse(exp).done.either.leftMap { s =>
      val msg = s"Failed to parse model expression: $exp. $s"
      LatisException(msg)
    }

  def unsafeParse(exp: String): DataType =
    ModelParser.parse(exp).fold(throw _, identity)

  def apply(exp: String): Either[LatisException, DataType] =
    ModelParser.parse(exp)

  private def variable: Parser[String] =
    sepBy1(identifier, char('.')).map(_.toList.mkString("."))

  private def dataType: Parser[DataType] =
    function | tuple | scalar

  /** Doesn't support nested functions in the domain. */
  def function: Parser[DataType] =
    parens(functionWithoutParens) | functionWithoutParens

  /** Doesn't support nested functions in the domain. */
  private def functionWithoutParens: Parser[DataType] = for {
    d <- (tuple | scalar).token
    _ <- string("->").token
    r <- dataType.token
  } yield Function(d, r)

  /** Only parses tuples of scalars */
  def tuple: Parser[DataType] = for {
    l <- parens(sepBy(scalar.token, char(',').token))
  } yield Tuple(l)

  def scalar: Parser[DataType] =
    scalarWithType | scalarWithoutType

  private def scalarWithType: Parser[DataType] = for {
    id <- identifier.token
    _  <- string(":").token
    t  <- valueType.token
  } yield Scalar(Metadata(id) + ("type" -> t))

  private def scalarWithoutType: Parser[DataType] = for {
    id <- identifier.token
  } yield Scalar(Metadata(id) + ("type" -> defaultScalarType))

  private def valueType: Parser[String] =
    stringCI("boolean") |
      stringCI("byte") |
      stringCI("char") |
      stringCI("short") |
      stringCI("int") |
      stringCI("long") |
      stringCI("float") |
      stringCI("double") |
      stringCI("binary") |
      stringCI("string") |
      stringCI("bigint") |
      stringCI("bigdecimal")
}

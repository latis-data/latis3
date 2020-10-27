package latis.model

import atto._
import Atto._
import cats.syntax.all._

import latis.metadata.Metadata
import latis.ops.parser.parsers.identifier
import latis.util.LatisException

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
    id <- variable.token
    _ <- string(":").token
    t <- valueType.token
  } yield Scalar(Metadata(id) + ("type" -> t.toString))

  private def scalarWithoutType: Parser[DataType] = for {
    id <- variable.token
  } yield Scalar(Metadata(id) + ("type" -> defaultScalarType))

  private def valueType: Parser[ValueType] = for {
    tName <- many(letter)
    tString = tName.mkString("")
  } yield ValueType.fromName(tString).fold(throw _, identity)
}

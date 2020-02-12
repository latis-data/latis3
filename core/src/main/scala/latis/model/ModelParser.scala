package latis.model

import atto._
import Atto._
import cats.implicits._

import latis.metadata.Metadata
import latis.util.LatisException

/**
 * Defines a parser that turns a functional data model expression
 * into a DataType.
 */
object ModelParser {

  def parse(exp: String): Either[LatisException, DataType] =
    phrase(dataType).parse(exp).done.either.leftMap { s =>
      val msg = s"Failed to parse model expression: $exp. $s"
      LatisException(msg)
    }

  private def identifier: Parser[String] = for {
    init <- letter | char('_')
    rest <- many(letterOrDigit | char('_'))
  } yield (init :: rest).mkString

  private def variable: Parser[String] =
    sepBy1(identifier, char('.')).map(_.toList.mkString("."))

  private def scalar: Parser[Scalar] = variable.map { id =>
    Scalar(Metadata(
      "id" -> id,
      "type" -> "double" //TODO: allow in spec
    ))
  }

  private def tuple: Parser[Tuple] = for {
    _ <- char('(')
    vars <- many(scalar)
    _ <- many(whitespace) ~ char(',') ~ many(whitespace)
    last <- scalar
    _ <- char(')')
  } yield Tuple(vars :+ last)

  private def function: Parser[Function] = for {
    domain <- scalar | tuple
    _ <- many(whitespace) ~ string("->") ~ many(whitespace)
    range <- dataType
  } yield Function(domain, range)

  private def dataType: Parser[DataType] =
    function | tuple | scalar
}

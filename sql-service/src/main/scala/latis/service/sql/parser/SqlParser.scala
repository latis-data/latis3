package latis.service.sql
package parser

import cats.data.NonEmptyList
import cats.parse.Numbers
import cats.parse.Parser
import cats.parse.Parser0
import cats.parse.Rfc5234
import cats.syntax.all.*

import latis.util.Identifier

import SqlServiceError.*

/**
 * A parser for a subset of SQL that can be turned into LaTiS
 * operations.
 */
object SqlParser {

  def parse(str: String): Either[ParseError, Query] =
    query.parseAll(str).leftMap(ParseError(_))

  /**
   * Convert a parser into a parser that consumes trailing whitespace
   * (horizontal and vertical).
   */
  private def token[A](parser: Parser[A]): Parser[A] =
    parser <* ws

  /**
   * Parse a string (case insensitive) and consume trailing whitespace
   * (horizontal and vertical).
   */
  private def token(str: String): Parser[Unit] =
    token(Parser.ignoreCase(str))

  private val ws: Parser0[Unit] =
    (Rfc5234.wsp | Rfc5234.lf | Rfc5234.crlf).rep0.void

  // NOTE: Under our definition, one or more underscores is a valid
  // LaTiS identifier, and I've kept that behavior here. I did not
  // allow periods.
  private val identifier: Parser[Identifier] = {
    val underscore = Parser.char('_')
    val first = (underscore | Rfc5234.alpha)
    val rest  = Parser.oneOf(
      underscore :: Rfc5234.alpha :: Rfc5234.digit :: Nil
    ).rep0

    val p = (first ~ rest).string.map { id =>
      Identifier.fromString(id).get
    }

    token(p)
  }

  private val select: Parser[Unit] = token("select")

  private val projection: Parser[List[Identifier]] = {
    val star = Parser.char('*').as(List.empty)
    val list = identifier.repSep(token(",")).map(_.toList)

    token(star | list)
  }

  private val from: Parser[Unit] = token("from")

  private val where: Parser[Unit] = token("where")

  val selection: Parser[NonEmptyList[Selection]] = {
    val and = token("and")

    val op = Parser.oneOf(
      token("=").as(Selection.Eq)
      :: token(">=").as(Selection.GtEq)
      :: token(">").as(Selection.Gt)
      :: token("<=").as(Selection.LtEq)
      :: token("<").as(Selection.Lt)
      :: Nil
    )

    val value: Parser[String] = token(Numbers.jsonNumber)

    (identifier ~ op ~ value).repSep(and).map {
      _.map {
        case ((id, p), v) => Selection(id, p, v)
      }
    }.withContext("selection")
  }

  private val query: Parser[Query] =
    (
      (ws.with1 *> select *> projection) ~
      (from *> identifier) ~
      (where *> selection).?
    ).map { case ((pj, ds), ss) =>
      Query(ds, pj, ss.map(_.toList).getOrElse(List.empty))
    }
}

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
    query.parseAll(str).leftMap(err => ParseError(err.toString))

  private val newlines: Parser0[Unit] =
    (Parser.char('\n') | Parser.string("\r\n")).rep0.void

  private val whitespace: Parser0[Unit] =
    Rfc5234.wsp.rep0.void

  // NOTE: Under our definition, one or more underscores is a valid
  // LaTiS identifier, and I've kept that behavior here. I did not
  // allow periods.
  private val identifier: Parser[Identifier] = {
    val underscore = Parser.char('_')
    val first = (underscore | Rfc5234.alpha)
    val rest  = Parser.oneOf(
      underscore :: Rfc5234.alpha :: Rfc5234.digit :: Nil
    ).rep0

    (first ~ rest).string.map { id =>
      Identifier.fromString(id).get
    }
  }

  private val select: Parser[Unit] =
    Parser.ignoreCase("select") <* whitespace

  private val projection: Parser[List[Identifier]] = {
    val star = Parser.char('*').as(List.empty)
    val list = identifier.repSep(Parser.char(',') <* whitespace).map(_.toList)

    (star | list) <* whitespace
  }

  private val from: Parser[Unit] =
    Parser.ignoreCase("from") <* whitespace

  private val where: Parser[Unit] =
    Parser.ignoreCase("where") <* whitespace

  val selection: Parser[NonEmptyList[Selection]] = {
    val and = Parser.ignoreCase("and") <* whitespace

    val op = Parser.oneOf(List(
      Parser.string("=").as(Selection.Eq),
      Parser.string(">=").as(Selection.GtEq),
      Parser.string(">").as(Selection.Gt),
      Parser.string("<=").as(Selection.LtEq),
      Parser.string("<").as(Selection.Lt)
    )) <* whitespace.?

    val value: Parser[String] = Numbers.jsonNumber

    ((identifier <* whitespace.?) ~ op ~ value).repSep(whitespace *> and).map {
      _.map {
        case ((id, p), v) => Selection(id, p, v)
      }
    }.withContext("selection")
  }

  private val query: Parser[Query] =
    (
      (select *> projection) ~
      (from *> identifier) ~
      (whitespace *> where *> selection).? <*
      newlines
    ).map { case ((pj, ds), ss) =>
      Query(ds, pj, ss.map(_.toList).getOrElse(List.empty))
    }
}

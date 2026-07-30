package latis.service.sql

import scala.util.control.NoStackTrace

import cats.parse.Parser

import latis.util.Identifier

enum SqlServiceError extends NoStackTrace {

  /** Error indicating that a dataset could not be found. */
  case DatasetResolutionFailure(dataset: Identifier)

  /** Error indicating an issue parsing the query. */
  case ParseError(unwrap: Parser.Error)
}

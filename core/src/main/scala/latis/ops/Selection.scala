package latis.ops

import atto.Atto._
import atto.ParseResult
import cats.syntax.all._

import latis.data._
import latis.model._
import latis.ops.parser.ast
import latis.ops.parser.parsers
import latis.util.LatisException

/**
 * Operation to keep only Samples that meet the given selection criterion.
 */
case class Selection(vname: String, operator: String, value: String) extends Filter {
  //TODO: use smaller types, enumerate operators?
  //TODO: enable IndexedFunction to use binary search...
  //TODO: support nested functions, all or none?
  //TODO: allow value to have units

  def getSelectionOp: Either[LatisException, ast.SelectionOp] =
    parsers.selectionOp.parseOnly(operator) match {
    case ParseResult.Done(_, o) => Right(o)
    case _ => Left(LatisException(s"failed to parse operator $operator"))
  }

  def getValue(model: DataType): Either[LatisException, Datum] = for {
    scalar <- getScalar(model)
    cdata <- scalar.convertValue(value).leftMap(LatisException(_))
  } yield cdata

  def getScalar(model: DataType): Either[LatisException, Scalar] = model.findVariable(vname) match {
    case Some(s: Scalar) => Right(s)
    case _ => Left(LatisException(s"Selection variable not found: $vname"))
  }

  def predicate(model: DataType): Sample => Boolean = {
    // Get the desired Scalar from the model
    //TODO: support aliases

    // Determine the Sample position of the selected variable
    val pos: SamplePosition = model.getPath(vname) match {
      case Some(p) =>
        p.length match {
          case 1 => p.head
          case _ =>
            val msg = "Selection does not support values in nested Functions."
            throw new UnsupportedOperationException(msg)
        }
      case None => ??? //shouldn't happen due to earlier check
    }

    val cdata = getValue(model).fold(throw _, identity)
    val scalar = getScalar(model).fold(throw _, identity)
    val ordering = scalar.ordering

    (sample: Sample) =>
      sample.getValue(pos) match {
        case Some(d: Datum) =>
          ordering
            .tryCompare(d, cdata)
            .map(matches)
            .getOrElse {
              // Not comparable
              val msg = s"Selection failed to compare values: $d, $cdata"
              throw new UnsupportedOperationException(msg)
            }
        case Some(_: SampledFunction) =>
          // Bug: Should not find SF at this position
          throw LatisException("Should not find SampledFunction at this position")
        case Some(_: TupleData) =>
          throw LatisException("Should not find TupleData at this position")
        case None =>
          // Bug: There should be a Datum at this position
          throw LatisException("Should find Datum at this position")
      }
  }

  /**
   * Helper function to determine if the value comparison
   * satisfies the selection operation.
   */
  private def matches(comparison: Int): Boolean =
    if (operator == "!=") {
      comparison != 0
    } else {
      (comparison < 0 && operator.contains("<")) ||
      (comparison > 0 && operator.contains(">")) ||
      (comparison == 0 && operator.contains("="))
    }
}

object Selection {

  def apply(expression: String): Selection = {
    //TODO: beef up expression parsing
    val ss = expression.split("\\s+") //split on whitespace
    Selection(ss(0), ss(1), ss(2))
  }

  def makeSelection(expression: String): Either[LatisException, Selection] =
    parsers.selection.parseOnly(expression) match {
      case ParseResult.Done(_, ast.Selection(vname, op, value)) => Right(Selection(vname, ast.prettyOp(op), value))
      case _ => Left(LatisException(s"Failed to parse expression $expression"))
    }
}

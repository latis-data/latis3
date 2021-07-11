package latis.ops

import atto.Atto._
import atto.ParseResult
import cats.syntax.all._

import latis.data._
import latis.model._
import latis.util.Identifier
import latis.util.LatisException
import latis.util.dap2.parser.ast
import latis.util.dap2.parser.parsers

/**
 * Operation to keep only Samples that meet the given selection criterion.
 */
case class Selection(id: Identifier, operator: ast.SelectionOp, value: String) extends Filter {
  //TODO: use smaller types, enumerate operators?
  //TODO: enable IndexedFunction to use binary search...
  //TODO: support nested functions, all or none?
  //TODO: allow value to have units
  //TODO: nearest (~) is not a filter
  //      makes sense only for variable in Cartesian domain
  //      transform "x ~ v" to another operation
  //TODO: support matches (=~)

  def getValue(model: DataType): Either[LatisException, Datum] = for {
    scalar <- getScalar(model)
    cdata <- scalar.convertValue(value).leftMap(LatisException(_))
  } yield cdata

  def getScalar(model: DataType): Either[LatisException, Scalar] = model.findVariable(id) match {
    case Some(s: Scalar) => Right(s)
    case _ => Left(LatisException(s"Selection variable not found: ${id.asString}"))
  }

  def predicate(model: DataType): Sample => Boolean = {
    // Get the desired Scalar from the model
    //TODO: support aliases

    // Determine the Sample position of the selected variable
    val pos: SamplePosition = model.findPath(id) match {
      case Some(p) =>
        p.length match {
          case 1 => p.head
          case _ =>
            val msg = "Selection does not support values in nested Functions."
            throw new UnsupportedOperationException(msg)
        }
      case None => ??? //shouldn't happen due to earlier check TODO: happened with selection on nonpresent ert in latis3-packets
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
        case Some(NullData) =>
          throw LatisException("Should not find NullData at this position")
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
    if (operator == ast.NeEq) {
      comparison != 0
    } else {
      (comparison < 0 && ast.prettyOp(operator).contains("<")) ||
      (comparison > 0 && ast.prettyOp(operator).contains(">")) ||
      (comparison == 0 && ast.prettyOp(operator).contains("="))
    }
}

object Selection {

  def fromArgs(args: List[String]): Either[LatisException, Selection] = args match {
    case expr :: Nil => makeSelection(expr)
    case i :: o :: v :: Nil => for {
      id <- Identifier.fromString(i).toRight(LatisException(s"'$i' is not a valid identifier"))
      op <- getSelectionOp(o)
    } yield Selection(id, op, v)
    case _ => Left(LatisException("Selection requires either one or three arguments"))
  }

  def getSelectionOp(op: String): Either[LatisException, ast.SelectionOp] =
    parsers.selectionOp.parseOnly(op) match {
      case ParseResult.Done(_, o) => Right(o)
      case _ => Left(LatisException(s"Failed to parse operator $op"))
    }

  def makeSelection(expression: String): Either[LatisException, Selection] =
    parsers.selection.parseOnly(expression) match {
      case ParseResult.Done(_, ast.Selection(id, op, value)) => Right(Selection(id, op, value))
      case _ => Left(LatisException(s"Failed to parse expression $expression"))
    }
}

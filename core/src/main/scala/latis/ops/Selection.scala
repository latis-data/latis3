package latis.ops

import atto.Atto.*
import atto.ParseResult
import cats.syntax.all.*

import latis.data.*
import latis.model.*
import latis.util.*
import latis.util.dap2.parser.ast
import latis.util.dap2.parser.parsers

/**
 * Operation to keep only Samples that meet the given selection criterion.
 *
 * If the target variable has 'binWidth' defined (in the units of that variable),
 * this applies bin semantics such that:
 *  - The data value is interpreted as the inclusive lower bound of the bin.
 *  - The bin width is added to the lower bound (assuming natural ordering) to define the exclusive upper bound.
 *  - Equality (as in = or ==) matches if the value is within the bin: [lower, upper).
 *  - Inequality operators will match if partially overlapping the bin (upper value excluded).
 * Bin semantics requires numeric data which will be handled as a Double.
 *
 * As a Filter, a predicate determines the fate of a Sample based only on the state of that Sample.
 */
case class Selection(id: Identifier, operator: ast.SelectionOp, value: String) extends Filter {
  //TODO: restrict construction
  //TODO: safer by construction with model: Scalar, op, Datum
  //TODO: enable IndexedFunction to use binary search...
  //TODO: support nested functions, all or none?
  //TODO: allow value to have units
  //TODO: support matches (=~)
  //TODO: support bounds variable (as opposed to fixed binWidth)

  def predicate(model: DataType): Either[LatisException, Sample => Boolean] = {
    // Determine the Sample position of the selected variable
    def getPosition: Either[LatisException, SamplePosition] =
      model.findPath(id)
        .toRight(LatisException(s"Variable not found: ${id.asString}"))
        .flatMap { path =>
          path.length match {
            case 1 => path.head.asRight
            case _ =>
              val msg = "Selection does not support values in nested Functions."
              LatisException(msg).asLeft
          }
        }

    // Get the target Scalar variable
    def getScalar: Either[LatisException, Scalar] = model.findVariable(id) match {
      case Some(s: Scalar) => s.asRight
      case _ => LatisException(s"Scalar variable not found: ${id.asString}").asLeft
    }

    // Define an internal predicate to filter a data value.
    //TODO: extend Either to data predicate methods
    def makePredicate(scalar: Scalar, cdata: Datum) =
      if (scalar.isBinned)
        Selection.datumPredicateWithBinning(scalar, operator, cdata)
      else Selection.datumPredicate(scalar, operator, cdata)

    for {
      pos    <- getPosition
      scalar <- getScalar
      cdata  <- scalar.convertValue(value)
      pred    = makePredicate(scalar, cdata)
    } yield {
      // Define the primary predicate for the Sample filter.
      (sample: Sample) =>
        sample.getValue(pos) match {
          case Some(d: Datum) => pred(d)
          case _ => throw LatisException(s"Invalid Data at sample position $pos") //bug
        }
    }
  }

}

object Selection {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

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

  /**
   * Makes a function that applies the selection filter to a data value of the target variable.
   */
  private[ops] def datumPredicate(
    scalar: Scalar,
    operator: ast.SelectionOp,
    value: Datum
  ): Datum => Boolean = {
    val ordering = scalar.ordering
    operator match {
      case ast.Eq   => (d: Datum) => ordering.equiv(d, value)
      case ast.EqEq => (d: Datum) => ordering.equiv(d, value)
      case ast.NeEq => (d: Datum) => ! ordering.equiv(d, value)
      case ast.Gt   => (d: Datum) => ordering.gt(d, value)
      case ast.GtEq => (d: Datum) => ordering.gteq(d, value)
      case ast.Lt   => (d: Datum) => ordering.lt(d, value)
      case ast.LtEq => (d: Datum) => ordering.lteq(d, value)
      case _ =>
        //TODO: prevent by construction
        throw LatisException(s"Unsupported selection operator: $ast.prettyOp(operator)") 
    }
  }

  /**
   * Makes a function for the given Scalar that takes a Datum of that 
   * type and applies binning semantics to return a finite non-empty 
   * Bounds of Doubles for that bin.
   *
   * This assumes that the Datum from the dataset represents the bin start.
   * The bin will be inclusive on the lower bound and exclusive on the upper.
   * Assumes numeric types which will be converted to a Double.
   */
  private[ops] def makeBounder(scalar: Scalar): Datum => Bounds[Double] = {
    //TODO: add binPosition enum for start|mid|end, assume start for now
    //TODO: support reverse ordering?
    val w = scalar.binWidth
      .getOrElse(throw LatisException("Bug: Making Bounder with no binWidth defined"))

    (datum: Datum) => {
      val d = scalar.valueAsDouble(datum)
      Bounds.of(d, d + w)
    }
  }

  /**
   * Makes a function that applies the selection filter to a data value of the target variable
   * with bin semantics.
   * 
   * Samples with bins that partially overlap the test value will be included.
   */
  private[ops] def datumPredicateWithBinning(
    scalar: Scalar,
    operator: ast.SelectionOp,
    value: Datum
  ): Datum => Boolean = {
    // Interpret the selection value as a double
    val dvalue: Double = scalar.valueAsDouble(value)

    // Make function to get Bounds for a given data value from the dataset
    val bounder: Datum => Bounds[Double] = makeBounder(scalar)

    // Make lean predicate based on selection operator
    operator match {
      case ast.Eq        => (d: Datum) => bounder(d).contains(dvalue)
      case ast.EqEq      => (d: Datum) => bounder(d).contains(dvalue)
      case ast.NeEq      => (d: Datum) => ! bounder(d).contains(dvalue)
      case ast.Gt | ast.GtEq => (d: Datum) => bounder(d) match {
        case NonEmptyBounds(_, upper) => upper.boundsUpper(dvalue)
        case _ => false //Bug: Bin width is expected to be greater than 0
      }
      case ast.Lt | ast.LtEq => (d: Datum) => bounder(d) match {
        case NonEmptyBounds(lower, _) => lower.boundsLower(dvalue)
        case _ => false //Bug: Bin width is expected to be greater than 0
      }
      case _ => 
        //TODO: prevent by construction
        throw LatisException(s"Unsupported selection operator: $ast.prettyOp(operator)") 
    }
  }
}

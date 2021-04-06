package latis.ops

import atto.Atto._
import cats.syntax.all._

import latis.data._
import latis.model._
import latis.util.Identifier
import latis.util.LatisException
import latis.util.dap2.parser.parsers.scalarArg

/**
 * Pivot the samples of a nested Function such that each outer sample becomes
 * a single sample with a range that is a Tuple of the range values
 * corresponding to the given domain values.
 *   a -> b -> (c, d, ...)  =>  a -> (b0_c, b0_d, ..., b1_c, b1_d, ...)
 * The given identifiers (vids) are used as range prefixes in the model. For example,
 * given vids ("Fe", "Mg"), and a dataset
 *   time -> line -> (flux, error)
 * The new dataset would then be
 *   time -> (Fe_flux, Fe_error, Mg_flux, Mg_error)
 * Current limitations/assumptions:
 * - One layer of nesting
 * - given values exist in every nested Function
 * - values and vids should have the same number of elements
 * - Nested tuples in the range will be flattened to a single tuple
 *
 * @param values strings of data values to match on the variable being pivoted.
 *               The strings are parsed into Datum using the model.
 * @param vids prefix for each pivot value to prepend to the range id
 */
case class Pivot(values: Seq[String], vids: Seq[String]) extends MapOperation {
  /*
   * TODO: Support general pivot with no specified values
   * unique domain values becomes a new range Tuple
   * requires inspecting data to get new range type
   *   violates ability to get model lazily
   * assume Cartesian: each outer sample contains complete set of inner Function samples
   */
  /*
   * TODO: consider the returned range having nested tuples where each inner-tuple
   *  corresponds to a pivoted value, and the vids are used to name the tuples
   *  instead of renaming the scalars. ex:
   * time -> line -> (flux, error)
   * becomes
   * time -> (Fe: (flux, error), Mg: (flux, error))
   * However, if the range is a single variable, this results in singleton tuples
   */

  /**
   * Create function for the MapOperation to apply to the Dataset Data.
   */
  def mapFunction(model: DataType): Sample => Sample = {
    case Sample(domain, RangeData(mf: MemoizedFunction)) =>
      val scalar = model match {
        case Function(_, Function(s: Scalar, _)) => s
      }
      // Eval nested Function at each requested value.
      val range = values.flatMap { value =>
        val rangeValues = for {
          v <- scalar.parseValue(value)
          r <- mf.eval(DomainData(v))
        } yield r
        rangeValues.fold(throw _, identity)
      }
      Sample(domain, RangeData(range))
    //TODO: deal with errors?
    //TODO: use Fill interpolation? or nearest-neighbor so users don't need to know exact values
  }

  /**
   * Define new model. The nested Function is replaced with a Tuple
   * containing Scalars for each of the requested samples.
   */
  override def applyToModel(model: DataType): Either[LatisException, DataType] = model match {
    case Function(domain, Function(_, r)) =>
      val ranges = for {
        vid <- vids
        s   <- r.getScalars
      } yield s.rename({
        val sId = s.id.fold("")(_.asString)
        Identifier.fromString(vid ++ "_" ++ sId).getOrElse {
          throw LatisException(s"Could not rename Scalar with invalid identifier: $sId")
        }
      })
      (ranges match {
        case s1 :: Nil => Function(domain, s1)
        case ss        => Function(domain, Tuple(ss))
      }).asRight
    case _ => Left(LatisException("Invalid DataType"))
  }
}

object Pivot {
  def fromArgs(args: List[String]): Either[LatisException, Pivot] = {
    // define a parser that doesn't allow nested lists
    val argParser = parens(sepBy(scalarArg.token, char(',').token))

    args match {
      case valuesStr :: vidsStr :: Nil =>
        for {
          values <- argParser
            .parseOnly(valuesStr)
            .option
            .toRight(LatisException(s"Failed to parse Pivot input 'values': $valuesStr"))
          vids <- argParser
            .parseOnly(vidsStr)
            .option
            .toRight(LatisException(s"Failed to parse Pivot input 'vids': $vidsStr"))
        } yield Pivot(values, vids)
      case _ => Left(LatisException("Pivot requires exactly two list arguments"))
    }
  }
}

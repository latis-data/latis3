package latis.ops

import latis.data._
import latis.model._


/**
 * Pivot the samples of a nested Function such that each outer sample becomes
 * a single sample with a range that is a Tuple of the range values 
 * corresponding to the given domain values. 
 * The given identifiers are used for the model.
 * Current limitations/assumptions:
 * - One layer of nesting
 * - nested function is Scalar -> Scalar, no Tuples
 * - given values exist in every nested Function (unless we have interpolation)
 * Note that it is not generally useful to pivot an outer Function into
 * a single Tuple, but no reason to exclude it.
 */
case class Pivot(values: Seq[Any], vids: Seq[String]) extends MapOperation {
  
  /*
   * TODO: Support general pivot with no specified values
   * unique domain values becomes a new range Tuple
   * requires inspecting data to get new range type
   *   violates ability to get model lazily
   * assume Cartesian: each outer sample contains complete set of inner Function samples
   */

  /**
   * Create function for the MapOperation to apply to the Dataset data (SampledFunction).
   */
  def makeMapFunction(model: DataType): Sample => Sample = 
    //Note, model not needed for pivot
    (sample: Sample) => sample match {
      case Sample(domain, RangeData(mf: MemoizedFunction)) =>
        Sample(domain, values.map( v => mf(DomainData(v)).head.range.head )) //eval nested Function at each requested value, may not be cheap
        //TODO: use Fill interpolation? or nearest-neighbor so users don't need to know exact values
        //TODO: requires same value type?
    }
    
    
  /**
   * Define new model. The nested Function is replaced with a Tuple
   * containing one Scalar for each of the requested samples.
   */
  override def applyToModel(model: DataType): DataType = {
    model match {
      case Function(domain, Function(_,r)) =>
        val range = Tuple(vids.map(id => r.rename(id)): _*) //preserve existing metadata, e.g. units
        Function(domain, range)
      case _ => ??? //invalid data type
    }
  }

}
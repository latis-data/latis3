package latis.ops

import latis.data._
import latis.model._

/**
 * A RangeOperation only modifies the range of a Dataset
 * leaving the domain set unchanged.
 */
trait RangeOperation extends UnaryOperation {
  
  def rangeFunction: RangeData => RangeData
  
  override def applyToData(data: SampledFunction, model: DataType) = data map {
    case Sample(d, r) => Sample(d, rangeFunction(r))
  }
  
  /*
   * Implications for Spark RDD
   * since we don't change the domain (key) there is no need for shuffling
   * use mapVaues!
   * mapRange(rangeFunction)
   * 
   * could this override map to call mapRange?
   */
}
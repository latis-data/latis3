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
  
}
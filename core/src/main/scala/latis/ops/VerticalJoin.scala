package latis.ops

import cats.syntax.all.*

import latis.model.DataType
import latis.util.LatisException

/**
 * Trait for Joins that combine Datasets "vertically".
 * 
 * This Join is vertical in the sense that it combines
 * like Datasets along the domain dimensions as opposed
 * to a HorizontalJoin which can combine unlike range
 * variables. A VerticalJoin requires that each dataset 
 * and the result has the same model.
 */
trait VerticalJoin extends Join {
  //TODO: assert that models are the same, compatible?
  
  // Model is the same and unchanged, use the first
  def applyToModel(model1: DataType, model2: DataType): 
    Either[LatisException, DataType] = model1.asRight
  
}

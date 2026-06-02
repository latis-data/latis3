package latis.ops

import cats.syntax.all.*

import latis.model.*
import latis.util.LatisException

/**
 * Trait for Joins that combine Datasets "vertically".
 *
 * This Join is vertical in the sense that it combines
 * like Datasets along the domain dimensions (e.g. appending
 * time series). A VerticalJoin requires that each dataset
 * has the same (or compatible) model. The resulting dataset
 * will have the model of the first dataset.
 */
trait VerticalJoin extends Join {
  //TODO: validate that models are the same, compatible

  // Model is the same and unchanged, use the first
  final def applyToModel(model1: DataType, model2: DataType):
    Either[LatisException, DataType] = {
    // Combine Scalars or Tuples as Function of Index
    if (model1.arity == 0) Function.from(Index(), model1)
    else model1.asRight
  }

}

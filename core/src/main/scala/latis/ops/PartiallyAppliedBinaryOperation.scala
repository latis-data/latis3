package latis.ops

import latis.model.Dataset

/**
 * Repackage a BinaryOperation with the initial Dataset
 * to become a UnaryOperation.
 */
case class PartiallyAppliedBinaryOperation(
  binOp: BinaryOperation, 
  dataset: Dataset
) extends UnaryOperation {
 
  //TODO: provide both left and right versions
  
  override def apply(ds: Dataset): Dataset = binOp(dataset, ds)
}

package latis.ops

import latis.data.SampledFunction
import latis.dataset._
import latis.model.DataType

/**
 * Wraps a BinaryOperation with the initial Dataset
 * to become a UnaryOperation.
 */
case class PartiallyAppliedBinaryOperation(
  binOp: BinaryOperation,
  dataset: Dataset
) extends UnaryOperation {
  //TODO: provide both left and right versions
  //TODO: capture 1st dataset in prov

  override def applyToModel(model: DataType): DataType =
    binOp.applyToModel(dataset.model, model)

  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    val data0 = dataset match {
      case ad: AdaptedDataset => ad.tap().data.asFunction
      case td: TappedDataset  => td.data.asFunction
    }
    binOp.applyToData(data0, data)
  }
}

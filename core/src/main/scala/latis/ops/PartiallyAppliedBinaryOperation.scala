package latis.ops

import latis.dataset.Dataset
import latis.model.DataType
import latis.data.SampledFunction
import latis.dataset.AdaptedDataset
import latis.dataset.UnadaptedDataset

/**
 * Repackage a BinaryOperation with the initial Dataset
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
      case ad: AdaptedDataset   => ad.unadapt.data
      case ud: UnadaptedDataset => ud.data
    }
    binOp.applyToData(dataset.model, data0, model, data)
  }
}

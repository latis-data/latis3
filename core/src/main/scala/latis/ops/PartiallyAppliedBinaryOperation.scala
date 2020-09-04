package latis.ops

import latis.data.SampledFunction
import latis.dataset._
import latis.model.DataType
import latis.util.LatisException

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

  override def applyToModel(model: DataType): Either[LatisException, DataType] =
    binOp.applyToModel(dataset.model, model)

  override def applyToData(data: SampledFunction, model: DataType): Either[LatisException, SampledFunction] = {
    val data0 = dataset match {
      case ad: AdaptedDataset => ad.tap().map(_.data.asFunction)
      case td: TappedDataset  => Right(td.data.asFunction)
    }
    data0.flatMap(binOp.applyToData(_, data))
  }
}

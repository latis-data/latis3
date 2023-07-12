package latis.ops

import cats.syntax.all.*

import latis.data.Data
import latis.dataset.*
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

  override def applyToData(data: Data, model: DataType): Either[LatisException, Data] = {
    (dataset match {
      case ad: AdaptedDataset => ad.tap().map(_.data)
      case td: TappedDataset  => td.data.asRight
      case _ => throw LatisException("Invalid dataset for PartiallyAppliedBinaryOperation")
    }).flatMap(binOp.applyToData(_, data))
  }
}

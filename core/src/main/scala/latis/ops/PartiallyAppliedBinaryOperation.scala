package latis.ops

import cats.syntax.all.*

import latis.data.*
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
  //TODO: method on BinaryOperation

  override def applyToModel(model: DataType): Either[LatisException, DataType] =
    binOp.applyToModel(dataset.model, model)

  override def applyToData(data: Data, model: DataType): Either[LatisException, Data] = {
    (dataset match {
      case ad: AdaptedDataset => ad.tap().map(_.samples)
      case td: TappedDataset  => td.samples.asRight
      case _ => LatisException("Invalid dataset for PartiallyAppliedBinaryOperation").asLeft
    }).flatMap { samples =>
      binOp.applyToData(dataset.model, samples, model, data.samples).map { ss =>
        StreamFunction(ss)
      }
    }
  }
}

package latis.ops

import latis.data.Data
import latis.data.DomainData
import latis.dataset.ComputationalDataset
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.model.DataType
import latis.model.Function
import latis.util.LatisException

case class Composition(dataset: Dataset) extends MapRangeOperation {

  override def applyToModel(model: DataType): DataType = {
    //TODO: make sure dataset range matches compFunction domain
    val domain = model match {
      case Function(d, _) => d
    }
    val range = dataset.model match {
      case Function(_, r) => r
    }
    Function(domain, range)
  }

  override def mapFunction(model: DataType): Data => Data = {
    //TODO: change SF.apply to use Data?
    val f: Data => Either[LatisException, Data] = (data: Data) =>
      dataset match {
        case ComputationalDataset(_, _, f) => f(data)
        case mds: MemoizedDataset => DomainData.fromData(data).flatMap { dd =>
          mds.data.asFunction.apply(dd).map(Data.fromSeq(_))
        }
    }
    (input: Data) =>
      f(input) match {
        case Right(x) => x
        case Left(le) => throw le
      }
  }
}

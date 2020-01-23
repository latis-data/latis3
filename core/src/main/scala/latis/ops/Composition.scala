package latis.ops

import latis.data.TupleData
import latis.dataset.DatasetFunction
import latis.model.DataType
import latis.model.Function

case class Composition(compFunction: DatasetFunction) extends MapRangeOperation {

  override def applyToModel(model: DataType): DataType = {
    //TODO: make sure dataset range matches compFunction domain
    val domain = model match {
      case Function(d, _) => d
    }
    val range = compFunction.model match {
      case Function(_, r) => r
    }
    Function(domain, range)
  }

  override def mapFunction(model: DataType): TupleData => TupleData = {
    //TODO: use TupleData since we to to feed the function any Data (e.g. spectrum)
    (input: TupleData) =>
      compFunction(input) match {
        case Right(x) => x
        case Left(le) => throw le
      }
  }
}

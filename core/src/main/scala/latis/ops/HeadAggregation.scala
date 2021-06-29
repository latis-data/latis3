package latis.ops

import cats.syntax.all._

import latis.data._
import latis.model._
import latis.util.LatisException

/**
 * Defines an Aggregation that reduces the Samples of a Dataset
 * to a single zero-arity Sample with the range of the first Sample.
 */
case class HeadAggregation() extends Aggregation {

  def aggregateFunction(model: DataType): Iterable[Sample] => Data =
    (samples: Iterable[Sample]) =>
      if (samples.isEmpty) applyToModel(model).fold(throw _, identity) match {
        //can't fill Function, use empty //TODO: NullData? not for outer function?
        case _: Function => SeqFunction(Seq.empty)
        case dt => dt.fillData
      }
      else Data.fromSeq(samples.head.range)

  def applyToModel(model:DataType): Either[LatisException, DataType] = model match {
    case Function(_, r) => r.asRight
    case _ => model.asRight
  }
}

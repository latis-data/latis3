package latis.ops

import cats.syntax.all.*

import latis.data.*
import latis.model.*
import latis.util.Identifier.*
import latis.util.LatisException

/**
 * Defines an Aggregation that reduces the Samples of a Dataset
 * to a variable capturing the number of Samples.
 */
class CountAggregation() extends Aggregation {

  def aggregateFunction(model: DataType): Iterable[Sample] => Data =
    samples => Data.LongValue(samples.size.toLong)

  def applyToModel(model:DataType): Either[LatisException, DataType] =
    Scalar(id"count", LongValueType).asRight

}

object CountAggregation {

  def builder: OperationBuilder = (args: List[String]) => {
    if (args.nonEmpty) LatisException("CountAggregation does not take arguments").asLeft
    else CountAggregation().asRight
  }

  def apply(): CountAggregation = new CountAggregation()
}

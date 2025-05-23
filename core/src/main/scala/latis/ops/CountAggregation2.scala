package latis.ops

import cats.effect.*
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.Identifier.*
import latis.util.LatisException

/**
 * Defines an Aggregation that reduces the Samples of a Dataset
 * to a variable capturing the number of Samples.
 */
class CountAggregation2() extends Aggregation2 {
  
  def aggregateFunction(model: DataType): Stream[IO, Sample] => IO[Data] =
    samples => samples.compile.count.map(Data.LongValue)

  def applyToModel(model:DataType): Either[LatisException, DataType] =
    Scalar(id"count", LongValueType).asRight

}

object CountAggregation2 {

  def builder: OperationBuilder = (args: List[String]) => {
    if (args.nonEmpty) LatisException("CountAggregation2 does not take arguments").asLeft
    else CountAggregation2().asRight
  }

  def apply(): CountAggregation2 = new CountAggregation2()
}

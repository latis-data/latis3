package latis.ops

import cats.effect.IO
import fs2.Pipe
import fs2.Stream

import latis.data.*
import latis.model.*
import latis.util.LatisException

/**
 * Reduces a Stream of Samples into a single Data object.
 * 
 * An Aggregation Operation combines all the Samples of a Dataset
 * into a Dataset with a single zero-arity Sample. The abstract
 * [[aggregationFunction]] can be used for other cases including
 * aggregating the results of a Group Operation.
 */
trait Aggregation2 extends StreamOperation { self =>
  //TODO: replace Aggregation with this (https://github.com/latis-data/latis3/issues/886)
  //TODO: ensure aggregated Data is memoized
  //TODO: be mindful of chunk sizes

  /** Define the function to combine samples */
  def aggregateFunction(model: DataType): Stream[IO, Sample] => IO[Data]

  /** Aggregate Samples and pack into a single Sample */
  override def pipe(model: DataType): Pipe[IO, Sample, Sample] =
    val aggF = aggregateFunction(model)
    samples => Stream.eval(
      aggF(samples).map(d => Sample(DomainData(), RangeData(d)))
    )

  def compose(mapOp: MapOperation): Aggregation2 = new Aggregation2 {

    def applyToModel(model: DataType): Either[LatisException, DataType] =
      mapOp.applyToModel(model).flatMap(self.applyToModel)

    def aggregateFunction(model: DataType): Stream[IO, Sample] => IO[Data] = {
      val mapF = mapOp.mapFunction(model)
      val aggF = mapOp.applyToModel(model).map(self.aggregateFunction)
      samples => {
        val tmpSamples = samples.map(mapF)
        IO.fromEither(aggF).flatMap(f => f(tmpSamples))
      }
    }
  }

}

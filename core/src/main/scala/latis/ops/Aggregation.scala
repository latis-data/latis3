package latis.ops

import cats.effect.IO
import fs2.Pipe
import fs2.Stream

import latis.data._
import latis.model._

/**
 * Defines an Operation that combines all the Samples of a Dataset
 * into a Dataset with a ConstantFunction containing the result.
 * The aggregate function can be used to aggregate the results
 * of a GroupOperation.
 */
trait Aggregation extends StreamOperation { self =>

  def aggregateFunction(model: DataType): Iterable[Sample] => Data

  override def pipe(model: DataType): Pipe[IO, Sample, Sample] = {
    val zero: MemoizedFunction = SeqFunction(Seq.empty)
    // Build up a MemoizedFunction by appending Samples
    val foldF = (mf: MemoizedFunction, sample: Sample) =>
      SeqFunction(mf.sampleSeq :+ sample)

    (samples: Stream[IO, Sample]) =>
      samples.fold(zero)(foldF).map { mf =>
        aggregateFunction(model)(mf.sampleSeq)
      }.map(d => Sample(DomainData(), RangeData(d)))
  }

  def compose(mapOp: MapOperation): Aggregation = new Aggregation {

    def applyToModel(model: DataType): DataType = {
      self.applyToModel(mapOp.applyToModel(model))
    }

    override def aggregateFunction(model: DataType): Iterable[Sample] => Data = {
      val tmpModel = mapOp.applyToModel(model)
      (samples: Iterable[Sample]) => {
        val tmpSamples = samples.map(mapOp.mapFunction(model))
        self.aggregateFunction(tmpModel)(tmpSamples)
      }
    }
  }

}

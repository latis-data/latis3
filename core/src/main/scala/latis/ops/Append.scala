package latis.ops

import cats.effect.*
import cats.syntax.all.*
import fs2.*

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException

/**
 * Joins two Datasets by appending their Streams of Samples.
 *
 * This assumes that the model of each dataset is the same and
 * uses the first. This assumes that there is no overlap in 
 * coverage, without confirming it. If there may be overlap, 
 * use SortedJoin.
 */
case class Append() extends VerticalJoin {
  //TODO: deal with non-Function Data: add index domain? error?

  override def applyToData(
    model1: DataType,
    stream1: Stream[IO, Sample],
    model2: DataType,
    stream2: Stream[IO, Sample],
  ): Either[LatisException, Stream[IO, Sample]] =
    (stream1 ++ stream2).asRight

  // Unused abstract method from Join
  // since a simple append can be optimized
  override def joinChunks(
    model1: DataType,
    c1: Chunk[Sample],
    model2: DataType,
    c2: Chunk[Sample]
  ): (Chunk[Sample], Chunk[Sample], Chunk[Sample]) =
    (Chunk.empty, Chunk.empty, Chunk.empty)
}

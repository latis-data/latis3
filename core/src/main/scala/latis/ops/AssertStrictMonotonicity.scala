package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.LatisException
import latis.util.LatisOrdering

/**
 * Operation to assert that Samples are ordered as they are streaming.
 *
 * The Stream will not be affected unless an ordering violation is
 * found, raising an error.
 *
 * Only the outer domain will be tested. A Scalar or Tuple will have
 * only one Sample so ordering is irrelevant.
 */
class AssertStrictMonotonicity extends StreamOperation {
  //TODO: test nested Functions, Tuples with Functions

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = (stream: Stream[IO, Sample]) =>
    val ordering = model match {
      case f: Function => LatisOrdering.sampleOrdering(f)
      case _ => LatisOrdering.defaultSampleOrdering //not used, only one sample
    }
    stream.zipWithPrevious.evalTap {
      case (Some(s1), s2) =>
        if (ordering.lt(s1, s2)) IO.unit
        else IO.raiseError {
          val msg = s"Samples are not strictly monotonic: ${s1.domain} ${s2.domain}"
          LatisException(msg)
        }
      case _ => IO.unit //first sample is fine
    }.map(_._2)

  def applyToModel(model: DataType): Either[LatisException, DataType] = model.asRight
}

object AssertStrictMonotonicity {

  //TODO: OperationBuilder, fromArgs
}

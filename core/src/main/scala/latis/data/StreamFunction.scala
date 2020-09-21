package latis.data

import cats.effect.IO
import fs2.Stream

import latis.util.DefaultDomainOrdering
import latis.util.LatisException
import latis.util.StreamUtils

/**
 * Implements a SampledFunction with an fs2.Stream of Samples.
 * Note that evaluation of a StreamFunction is limited by
 * being traversable once.
 * A Dataset can be memoized with "force" to ensure that it has a
 * MemoizedFunction that can be more generally evaluated.
 */
case class StreamFunction(samples: Stream[IO, Sample]) extends SampledFunction {

  def ordering: Option[PartialOrdering[DomainData]] = None //TODO: allow ord arg

  def eval(data: DomainData): Either[LatisException, RangeData] = {
    //TODO: can't do it once? make Evaluation "rewind" the dataset
    val ord: PartialOrdering[DomainData] = ordering.getOrElse(DefaultDomainOrdering)
    //TODO: avoid unsafe run
    StreamUtils.unsafeStreamToSeq(samples.find(s => ord.equiv(s.domain, data))).headOption match {
      case Some(Sample(_, rd)) => Right(rd)
      case None =>
        val msg = s"No sample found matching $data"
        Left(LatisException(msg))
    }
  }

  override def resample(domainSet: DomainSet): Either[LatisException, SampledFunction] =
    Left(LatisException("Can't resample a StreamFunction, for now"))
}

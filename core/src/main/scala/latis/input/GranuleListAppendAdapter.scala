package latis.input

import java.net.URI

import cats.implicits._
import cats.effect.IO
import fs2.Stream

import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.data.Text
import latis.dataset.Dataset
import latis.ops.Operation
import latis.util.LatisException
import latis.util.NetUtils

/**
 * An adapter for creating a dataset from a list of granules.
 *
 * This adapter creates a dataset by applying a template adapter to
 * each granule in a granule list dataset and appending the results.
 *
 * The granule list dataset is expected to be of the form `(d,,1,,,
 * ..., d,,n,,) -> uri` or `(d,,1,,, ..., d,,n,,) -> (uri, ...)`.
 */
final class GranuleListAppendAdapter(granules: Dataset, template: URI => Dataset) {

  /** Gets data using this adapter. */
  def getData(ops: Seq[Operation]): SampledFunction = {
    val samples: Stream[IO, Sample] = for {
      sample <- granules.samples
      uri    <- Stream.fromEither[IO](extractUri(sample))
      dataset = template(uri)
      result <- dataset.samples
    } yield result

    SampledFunction(samples)
  }

  private def extractUri(s: Sample): Either[LatisException, URI] = s match {
    case Sample(_, RangeData(Text(uri)))    => NetUtils.parseUri(uri)
    case Sample(_, RangeData(Text(uri), _)) => NetUtils.parseUri(uri)
    case _ => LatisException("Expected sample with granule URI").asLeft
  }
}

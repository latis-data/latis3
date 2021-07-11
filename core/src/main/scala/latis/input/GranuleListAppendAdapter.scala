package latis.input

import java.net.URI

import scala.annotation.unused

import cats.implicits._
import cats.effect.IO
import fs2.Stream

import latis.data.Sample
import latis.data.SampledFunction
import latis.data.SamplePosition
import latis.data.Text
import latis.dataset.Dataset
import latis.ops.Operation
import latis.util.Identifier.IdentifierStringContext
import latis.util.LatisException
import latis.util.NetUtils

/**
 * An adapter for creating a dataset from a list of granules.
 *
 * This adapter creates a dataset by applying a template adapter to
 * each granule in a granule list dataset and appending the results.
 *
 * The granule list dataset is expected to be of the form `(d,,1,,,
 * ..., d,,n,,) -> (..., uri, ...)`.
 */
final class GranuleListAppendAdapter(granules: Dataset, template: URI => Dataset) {

  /** Gets data using this adapter. */
  def getData(
    @unused ops: Seq[Operation]
  ): SampledFunction = {
    val samples: Stream[IO, Sample] = for {
      sample <- granules.samples
      pos    <- Stream.fromEither[IO] {
        granules.model.findPath(id"uri").toRight(
          LatisException("Expected 'uri' variable")
        ).flatMap {
          case p :: Nil => p.asRight
          case _ => LatisException("Expected non-nested 'uri' variable").asLeft
        }
      }
      uri    <- Stream.fromEither[IO](extractUri(sample, pos))
      dataset = template(uri)
      result <- dataset.samples
    } yield result

    SampledFunction(samples)
  }

  private def extractUri(
    s: Sample,
    p: SamplePosition
  ): Either[LatisException, URI] = s.getValue(p).toRight {
    LatisException("Expected sample with granule URI")
  }.flatMap {
    case Text(uri) => NetUtils.parseUri(uri)
    case _ => LatisException("Expected text URI variable").asLeft
  }
}

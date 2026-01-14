package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.*

import latis.data.Datum
import latis.data.Sample
import latis.data.SamplePosition
import latis.model.DataType
import latis.util.Identifier
import latis.util.LatisException

/**
 * Keep only samples where the given variable changes value.
 *
 * This keeps the first sample and drops any subsequent sample where the
 * given variable value is repeated, keeping only the samples where the
 * variable changes value. To preserve the coverage extents, the last
 * sample will be kept.
 *
 * This requires a flat dataset (no nested Functions). It doesn't really
 * make sense to apply to the domain, but could fix "broken" data with
 * non-distinct domain values (e.g. time series with duplicate times).
 */
class OnChange private (variableID: Identifier) extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = {
    // Get the variable position in a Sample. Must not be nested.
    val position = model.findPath(variableID) match {
      case Some(pos :: Nil) => pos
      case Some(path) if (path.size > 1) =>
        val msg = "OnChange variable must not be nested"
        throw LatisException(msg)
      case _ =>
        val msg = s"OnChange variable not found: ${variableID.asString}"
        throw LatisException(msg)
    }

    // Define Pipe
    (stream: Stream[IO, Sample]) => {
      // Loop, dropping samples that have the same target variable value.
      def go(prev: Sample, rest: Stream[IO, Sample]): Pull[IO, Sample, Unit] = {
        rest.pull.uncons1.flatMap {
          case Some((curr, tail)) =>
            if (getValue(prev, position) != getValue(curr, position)) {
              Pull.output1(curr) >> go(curr, tail)
            } else {
              // Unchanged, make sure to keep last sample to preserve coverage
              tail.pull.peek1.flatMap {
                case Some(_) => go(curr, tail)
                case None    => Pull.output1(curr) >> Pull.done
              }
            }
          case None => Pull.done
        }
      }

      // Get and emit first sample then start iterating
      stream.pull.uncons1.flatMap {
        case Some((first, tail)) =>
          Pull.output1(first) >> go(first, tail)
        case None =>
          Pull.done
      }.stream
    }
  }

  // Get the variable data from a Sample given its position
  private def getValue(sample: Sample, position: SamplePosition): Any =
    sample.getValue(position) match {
      case Some(d: Datum) => d.value
      case _ =>
        val msg = "OnChange variable value must be a Datum"
        throw LatisException(msg)
    }

  // Model is unchanged
  override def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight
}

object OnChange {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, OnChange] = args match {
    case name :: Nil =>
      Identifier.fromString(name).map(OnChange(_))
        .toRight(LatisException("OnChange: Invalid identifier"))
    case _ =>
      val msg = "OnChange requires a single variable argument"
      LatisException(msg).asLeft
  }
}

package latis.ops

import cats.data.ValidatedNel
import cats.effect.IO
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.Identifier
import latis.util.LatisException

/**
 * Removes a Sample if the value of the given numeric variable has
 * changed more than a given limit.
 *
 * This will include the first sample so it won't be useful if the
 * first sample is an outlier. This does not support nested variables.
 */
case class MaxDelta(id: Identifier, delta: Double) extends StreamOperation {

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = {
    if (validate(model).isInvalid) throw LatisException("Invalid MaxDelta")

    // Get the non-nested variable position in a Sample
    val position = model.findPath(id) match {
      case Some(pos :: Nil) => pos
      case Some(_) =>
        val msg = "MaxDelta variable must not be nested"
        throw LatisException(msg) //Bug: shouldn't validate
      case _ =>
        val msg = s"MaxDelta variable not found: $id}"
        throw LatisException(msg) //Bug: shouldn't validate
    }

    // Define Pipe
    (stream: Stream[IO, Sample]) => {
      // Loop, dropping samples that have large changes of the target variable.
      def go(prev: Sample, rest: Stream[IO, Sample]): Pull[IO, Sample, Unit] = {
        rest.pull.uncons1.flatMap {
          case Some((curr, tail)) =>
            val d = Math.abs(getValue(curr, position) - getValue(prev, position))
            if (d > delta) go(prev, tail) //drop sample
            else Pull.output1(curr) >> go(curr, tail)
          case None => Pull.done
        }
      }

      // Get and emit first sample then start iterating
      stream.pull.uncons1.flatMap {
        case Some((first, tail)) => Pull.output1(first) >> go(first, tail)
        case None => Pull.done
      }.stream
    }
  }

  // Get the variable value from a Sample given its position
  private def getValue(sample: Sample, position: SamplePosition): Double =
    sample.getValue(position) match {
      case Some(Number(d)) => d
      case _ =>
        val msg = "MaxDelta variable value must be numeric"
        throw LatisException(msg)
    }

  // Model is not affected
  override def applyToModel(model: DataType): Either[LatisException, DataType] =
    validate(model)
      .leftMap(es => es.head).toEither // Only one Exception
      .map(_ => model)

  // Make sure this operation is valid for this dataset
  def validate(model: DataType): ValidatedNel[LatisException, MaxDelta] = {
    val varIsNumeric = model.findVariable(id) match {
      case Some(s: Scalar) =>
        if (s.valueType.isInstanceOf[NumericType]) this.validNel
        else LatisException("MaxDelta variable must be numeric").invalidNel
      case _ => LatisException("MaxDelta scalar variable not found").invalidNel
    }

    val notNested = model.findPath(id) match {
      case Some(path) if (path.size == 1) => this.validNel
      case Some(_) => LatisException("MaxDelta variable must not be nested").invalidNel
      case None => LatisException("MaxDelta variable not found").invalidNel
    }
    
    varIsNumeric.andThen(_ => notNested)
  }

}

object MaxDelta {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, MaxDelta] = args match {
    case name :: delta ::Nil =>
      for {
        id <- Identifier.fromString(name)
          .toRight(LatisException("MaxDelta identifier invalid"))
        d <- delta.toDoubleOption
          .toRight(LatisException("MaxDelta value must be numeric"))
      } yield MaxDelta(id, d)
    case _ =>
      val msg = "MaxDelta requires a variable id and delta arguments"
      LatisException(msg).asLeft
  }
}

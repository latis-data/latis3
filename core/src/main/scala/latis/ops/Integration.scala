package latis.ops

import scala.annotation.tailrec

import cats.data.ValidatedNel
import cats.effect.IO
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.Identifier
import latis.util.LatisException

/**
 * Reduces a Function to a Scalar or Tuple by applying an integration
 * algorithm.
 *
 * This expects a (potentially nested) Function with the given Scalar
 * (variable of integration) as the only domain variable. The range of
 * said Function must have numeric Scalars only.
 */
trait Integration extends StreamOperation {
  //TODO: note similarity to Aggregation: samples => Data

  /** Variable of integration */
  def id: Identifier

  /**
   * Integration algorithm
   *
   * Reduce the samples into a single Data object.
   * Should only be called for a 1D Function with numeric range data.
   */
  def integrate(model: DataType, samples: Stream[IO, Sample]): IO[Data]


  override def pipe(model: DataType): Pipe[IO, Sample, Sample] = {
    def go(model: DataType, samples: Stream[IO, Sample]): IO[Data] = {
      model match {
        case Function(domain: Scalar, range) if (domain.id == id) =>
          integrate(model, samples)
        case Function(domain, range: Function) =>
          val ss = samples.flatMap {
            case Sample(d, RangeData(f: SampledFunction)) =>
              Stream.eval(go(range, f.samples).map(r => Sample(d, RangeData(r))))
          }
          // Memoize nested Function
          ss.compile.toList.map(SampledFunction.apply)
        case _ => throw LatisException("Failed to find variable of integration")
      }
    }

    samples => Stream.eval(go(model, samples)).flatMap(_.samples)
  }

  override def applyToModel(model: DataType): Either[LatisException, DataType] = {
    def go(model: DataType): Either[LatisException, DataType] = {
      model match {
        case Function(domain: Scalar, range) if (domain.id == id) => range.asRight
        case Function(domain, range: Function) =>
          go(range).flatMap(Function.from(domain, _))
        case _ => LatisException("Failed to find variable of integration").asLeft
      }
    }

    validate(model).toEither.leftMap(_.head).flatMap(_ => go(model))
  }

  def validate(model: DataType): ValidatedNel[LatisException, Integration] = {
    @tailrec
    def go(model: DataType): ValidatedNel[LatisException, Integration] = {
      model match {
        case Function(domain: Scalar, range) if (domain.id == id) =>
          if (isNumeric(range)) this.validNel
          else LatisException("Integration requires numeric range data").invalidNel
        case Function(domain, range: Function) => go(range)
        case _ => LatisException("Failed to find variable of integration").invalidNel
      }
    }
    go(model)
  }

  //TODO: util?
  private def isNumeric(model: DataType): Boolean = {
    model match {
      case s: Scalar   => s.valueType.isInstanceOf[NumericType]
      case Tuple(es *) => es.forall(isNumeric) //not tail recursive, but shallow
      case _: Function => false
    }
  }
}

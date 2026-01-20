package latis.ops

import cats.data.ValidatedNel
import cats.effect.IO
import cats.kernel.PartialOrder
import cats.syntax.all.*
import cats.Eq
import fs2.Pipe

import latis.data.*
import latis.model.*
import latis.util.CartesianDomainOrdering
import latis.util.LatisException

/**
 * Make a nested Function using the right n domain variables.
 *
 * n must be greater than 0 and less than the arity of the Dataset.
 * This requires a Cartesian Dataset to ensure that uniqueness
 * and order are preserved.
 */
case class CurryRight(n: Int) extends StreamOperation {
  //TODO: handle nested tuple in domain
  //  note arity <= dimensionality
  //TODO: impl Curry with this with arity - n
  //  smart constructor, but need model
  //TODO: does it make sense to support n = 0 or arity? no-op

  override def pipe(model: DataType): Pipe[IO, Sample, Sample] = samples => {

    // Define Eq for the new outer domain
    given Eq[DomainData] = PartialOrder.fromPartialOrdering {
      model match {
        case Function(domain, _) =>
          CartesianDomainOrdering(domain.getScalars.dropRight(n).map(_.ordering))
        case _ => throw LatisException("Bug: validation should have caught this")
      }
    }

    // Collect all the inner samples for each outer sample
    // then make a new sample with a nested function
    samples.groupAdjacentBy {
      case Sample(ds, _) => ds.dropRight(n)
    }.map {
      case (domain, samples) =>
        val innerSamples = samples.toList.map {
          case Sample(d, r) => Sample(d.takeRight(n), r)
        }
        Sample(domain, RangeData(SampledFunction(innerSamples)))
    }
  }

  // Reshape model with nested Function
  override def applyToModel(model: DataType): Either[LatisException, DataType] = {
    validate(model).toEither.leftMap(_.head).flatMap { _ =>
      model match {
        case Function(domain, range) =>
          val (ss1, ss2) = domain.getScalars.splitAt(n)
          for {
            d1 <- makeDomain(ss1)
            d2 <- makeDomain(ss2)
            in <- Function.from(d2, range)
            f <- Function.from(d1, in)
          } yield f
        case _ => throw LatisException("Bug: validation should have caught this")
      }
    }
  }

  // Make a domain type from a list of Scalars
  //TODO: util? we do this a lot
  private def makeDomain(scalars: List[Scalar]) = scalars match {
    case Nil => throw LatisException("Bug: validation should have caught this")
    case s :: Nil => s.asRight
    case ss => Tuple.fromSeq(ss)
  }

  def validate(model: DataType): ValidatedNel[LatisException, CurryRight] = {
    //TODO: require topology = cartesian property on Function
    model match {
      case Function(domain, _) =>
        if (n > 0 && n < domain.getScalars.size) this.validNel
        else LatisException("n must be between 0 and the arity").invalidNel
      case _ =>
        LatisException("Curry expects a Function").invalidNel
    }
  }
}

object CurryRight {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, CurryRight] = args match {
    case n :: Nil => Either.catchOnly[NumberFormatException](CurryRight(n.toInt))
      .leftMap(LatisException(_))
    case _ => LatisException("CurryRight expects a single integer argument").asLeft
  }
}
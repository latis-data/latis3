package latis.ops

import cats.data.ValidatedNel
import cats.effect.*
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.Identifier
import latis.util.LatisException

/**
 * Defines an operation that evaluates a Dataset at a given value
 * for a given domain variable, returning a new Dataset keeping only
 * the samples with that domain value. That domain dimension will be
 * removed, reducing the arity of the dataset by one.
 *
 * This requires that the Dataset has a Function data model with
 * the given id corresponding to a domain Scalar in the outer domain
 * (not within a nested Function, for now). This requires that the
 * domain set is Cartesian so uniqueness and ordering can be preserved.
 */
case class Evaluation(id: Identifier, value: String) extends StreamOperation {
  //TODO: allow interpolation?

  // Remove the dimension being evaluated
  def applyToModel(model: DataType): Either[LatisException, DataType] = {
    validate(model).toEither.leftMap(_.head).flatMap { _ =>
      val (domain, range) = model match {
        case Function(domain, range) => (domain, range)
        case _ => throw LatisException("Bug: Validation should have caught this")
      }
      val scalars = domain.getScalars.filterNot(_.id == id)
      scalars.length match {
        case 0 => range.asRight //no longer a Function
        case 1 => Function.from(scalars.head, range)
        case _ => Tuple.fromSeq(scalars).flatMap { domain =>
          Function.from(domain, range)
        }
      }
    }
  }

  // Keep only the samples with the matching domain value
  // and remove that element from the domain.
  def pipe(model: DataType): Pipe[IO, Sample, Sample] = {
    // Note, these should not throw if validation succeeded
    val pos = domainPosition(model, id)
    val domainVar = model.getScalars(pos)
    val ordering = domainVar.ordering
    val data = domainVar.parseValue(value).fold(throw _, identity) //validated

    samples => samples.map {
      case Sample(domain, range) =>
        domain.get(pos) match {
          case Some(d) =>
            if (ordering.equiv(d, data)) {
              val (before, after) = domain.splitAt(pos)
              Sample(before ++ after.drop(1), range).some
            } else None
          case None => None //drop invalid sample, log?
        }
    }.unNone
  }

  // Get the sample position of the given variable in the model.
  private def domainPosition(model: DataType, id: Identifier): Int =
    model.findPath(id).get match {
      case DomainPosition(p) :: Nil => p
      case _ => throw LatisException("Bug: Validation should have caught this")
    }

  // Make sure this Operation is applicable for a Dataset with the given model
  private def validate(model: DataType): ValidatedNel[LatisException, Evaluation] = {
    model match {
      case Function(domain, _) =>
        domain.findVariable(id) match {
          case Some(s: Scalar) =>
            if (s.parseValue(value).isRight) this.validNel
            else {
              val msg = s"Invalid value: $value for variable $id"
              LatisException(msg).invalidNel
            }
          case _ =>
            val msg = s"Evaluation variable not found in domain: $id"
            LatisException(msg).invalidNel
        }
      case _ => LatisException("Evaluation requires a Function").invalidNel
    }
  }

}

object Evaluation {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, Evaluation] = args match {
    case variable :: value :: Nil =>
      Identifier.fromString(variable).map { id =>
        Evaluation(id, value)
      }.toRight(LatisException(s"Invalid identifier $variable"))
    case _ => Left(LatisException("Evaluation requires a variable name and value"))
  }
}

package latis.ops

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
 * This assumes the Dataset is uncurried (no nested Functions).
 */
case class Evaluation(id: Identifier, value: String) extends StreamOperation {

  // Remove the dimension being evaluated
  def applyToModel(model: DataType): Either[LatisException, DataType] = {
    for {
      d_r <- model match { //TODO: why can't we map into (domain, range)?
        case Function(domain, range) => (domain, range).asRight
        case _ => LatisException("Evaluation requires a Function").asLeft
      }
      pos <- model.findPath(id).flatMap {
        case DomainPosition(p) :: Nil => p.some
        case _ => None
      }.toRight(LatisException(s"Evaluation domain variable not found: $id"))
      (before, after) = d_r._1.getScalars.splitAt(pos)
      scalars = before ++ after.drop(1)
      func <- scalars.length match {
        case 0 => d_r._2.asRight
        case 1 => Function.from(scalars.head, d_r._2)
        case _ => Tuple.fromSeq(scalars).flatMap { domain =>
          Function.from(domain, d_r._2)
        }
      }
    } yield func
  }

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = {
    val pos = domainPosition(model, id).fold(throw _, identity)
    val domainVar = model.getScalars(pos)
    val ordering = domainVar.ordering
    val data = domainVar.parseValue(value).fold(throw _, identity)

    samples => samples.map {
      case Sample(domain, range) =>
        domain.get(pos) match {
          case Some(d) =>
            if (ordering.equiv(d, data)) {
              val (before, after) = domain.splitAt(pos)
              Sample(before ++ after.drop(1), range).some
            } else None
          case None => None //drop unexpected sample
        }
    }.unNone
  }

  private def domainPosition(model: DataType, id: Identifier): Either[LatisException, Int] = {
    for {
      domain <- model match {
        case Function(domain, _) => domain.asRight
        case _ => LatisException("Evaluation requires a Function").asLeft
      }
      pos <- model.findPath(id).flatMap {
        case DomainPosition(p) :: Nil => p.some
        case _ => None
      }.toRight(LatisException(s"Evaluation domain variable not found: $id"))
    } yield pos
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

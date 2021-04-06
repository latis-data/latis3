package latis.ops

import cats.syntax.all._

import latis.data._
import latis.model._
import latis.util.LatisException

/**
 * Defines an operation that evaluates a Dataset at a given value
 * returning a new Dataset encapsulating the range value.
 */
case class Evaluation(value: String) extends UnaryOperation {
  //TODO: assert that data is of the right type based on the model domain
  //TODO: Allow parsing multiple values for higher arity datasets.

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    (model match {
      case Function(_, r) => r
      case _              => model
    }).asRight

  def applyToData(data: Data, model: DataType): Either[LatisException, Data] =
    model.arity match {
      case 0 => data.asRight
      case 1 =>
        for {
          v  <- model.getScalars.head.parseValue(value)
          dd <- DomainData.fromData(v)
          rd <- data.eval(dd)
        } yield Data.fromSeq(rd)
      case _ => LatisException("Can't evaluate a multi-dimensional dataset").asLeft
    }
}

object Evaluation {
  def fromArgs(args: List[String]): Either[LatisException, Evaluation] = args match {
    case arg :: Nil => Right(Evaluation(arg))
    case _          => Left(LatisException("Evaluation requires exactly one argument"))
  }
}

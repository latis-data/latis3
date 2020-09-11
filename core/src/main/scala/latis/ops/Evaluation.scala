package latis.ops

import latis.data._
import latis.model._
import latis.util.LatisException

/**
 * Defines an operation that evaluates a Dataset at a given value
 * returning a new Dataset encapsulating the range value as a
 * ConstantFunction.
 */
case class Evaluation(data: String) extends UnaryOperation {
  //TODO: assert that data is of the right type based on the model domain

  def applyToModel(model: DataType): Either[LatisException, DataType] =
    Right(model match {
      case Function(_, r) => r
      case _ => model
    })

  def applyToData(
    sf: SampledFunction,
    model: DataType
  ): Either[LatisException, SampledFunction] =
    model.arity match {
      case 0 => Right(sf)
      case 1 => for {
        v  <- model.getScalars.head.parseValue(data)
        // TODO: Allow parsing multiple values for higher arity datasets.
        dd <- DomainData.fromData(v)
        rd <- sf(dd)
        d   = Data.fromSeq(rd)
      } yield ConstantFunction(d)
      case _ => Left(LatisException("Can't evaluate a multi-dimensional dataset"))
    }
}

object Evaluation {

  def fromArgs(args: List[String]): Either[LatisException, Evaluation] = args match {
    case arg :: Nil => Right(Evaluation(arg))
    case _ => Left(LatisException("Evaluation requires exactly one argument"))
  }
}

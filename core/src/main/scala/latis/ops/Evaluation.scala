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

  def applyToModel(model: DataType): DataType = {
    //TODO: assert that data is of the right type based on the model domain
    model match {
      case Function(_, r) => r
      case _ => model
    }
  }

  def applyToData(sf: SampledFunction, model: DataType): SampledFunction = sf match {
    case cf: ConstantFunction => cf
    case sf: SampledFunction =>
      val ecf = for {
        v  <- model.getScalars.head.parseValue(data)
        dd <- DomainData.fromData(v)
        rd <- sf(dd)
        d   = Data.fromSeq(rd)
      } yield ConstantFunction(d)
      ecf.toTry.get //throw the exception if Left
  }
}

object Evaluation {

  def fromArgs(args: List[String]): Either[LatisException, Evaluation] = args match {
    case arg :: Nil => Right(Evaluation(arg))
    case _ => Left(LatisException("Evaluation requires exactly one argument"))
  }
}

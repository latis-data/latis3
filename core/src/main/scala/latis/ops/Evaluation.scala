package latis.ops

import latis.data._
import latis.model._

/**
 * Defines an operation that evaluates a Dataset at a given value
 * returning a new Dataset encapsulatng the range value as a
 * ConstantFunction.
 */
case class Evaluation(data: Data) extends UnaryOperation {

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
        dd <- DomainData.fromData(data)
        rd <- sf(dd)
        d   = Data.fromSeq(rd)
      } yield ConstantFunction(d)
      ecf.toTry.get //throw the exception if Left
  }
}

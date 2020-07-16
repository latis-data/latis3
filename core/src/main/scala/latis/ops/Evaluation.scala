package latis.ops

import latis.data._
import latis.model._

/**
 * Defines an operation that evaluates a Dataset at a given value
 * returning a new Dataset.
 */
case class Evaluation(domainData: Data) extends UnaryOperation {

  def applyToModel(model: DataType): DataType =
    //TODO: assert that data is of the right type based on the model domain
    model match {
      case Function(_, r) => r
      case _              => model
    }

  def applyToData(data: Data, model: DataType): Data =
    data match {
      case sf: SampledFunction =>
        val ecf = for {
          dd <- DomainData.fromData(domainData)
          rd <- sf(dd)
          d = Data.fromSeq(rd)
        } yield d
        ecf.toTry.get //throw the exception if Left
      // Other Data are effectively constant functions
      case const => const
    }
}

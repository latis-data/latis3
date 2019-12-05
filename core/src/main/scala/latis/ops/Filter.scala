package latis.ops

import latis.data._
import latis.model._

/**
 * A Filter is a unary Operation that applies a boolean
 * predicate to each Sample of the given Dataset resulting
 * in a new Dataset that has all the "false" Samples removed.
 * This only impacts the number of Samples in the Dataset.
 * It does not affect the model.
 */
trait Filter extends UnaryOperation with StreamingOperation { self =>
  //TODO: update "length" metadata?
  //TODO: clarify behavior of nested Functions: all or none

  /**
   * Create a function that specifies whether a Sample
   * should be kept.
   */
  def makePredicate(model: DataType): Sample => Boolean
  //TODO: just "predicate"?

  /**
   * Compose with a MappingOperation.
   * Note that the MappingOperation will be applied first.
   * This satisfies the StreamingOperation trait.
   */
  def compose(mappingOp: MapOperation): Filter = new Filter {
    //TODO: apply to metadata

    def makePredicate(model: DataType): Sample => Boolean =
      mappingOp.mapFunction(model).andThen(self.makePredicate(mappingOp.applyToModel(model)))

    // Note, the Filter Operation does not affect the model.
    override def applyToModel(model: DataType): DataType =
      mappingOp.applyToModel(model)
  }

  /**
   * Delegate to the Dataset's SampledFunction to apply the predicate
   * and generate a new SampledFunction
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction =
    data.filter(makePredicate(model))

}

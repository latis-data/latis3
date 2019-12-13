package latis.ops

import latis.data._
import latis.model._
import latis.util.LatisException
import latis.util.LatisOrdering

trait GroupingOperation extends UnaryOperation with StreamingOperation { self =>

  /**
   * Specify an Aggregation Operation to use to reduce
   * a collection of Samples to a single RangeData.
   */
  def aggregation: Aggregation
  //TODO: andThen(op)

  /**
   * Define a function that optionally creates a DomainData
   * from a Sample. These DomainData be used to define the
   * domain set of the resulting SampledFunction.
   */
  def groupByFunction(model: DataType): Sample => Option[DomainData]

  /**
   * Compose with a MappingOperation.
   * Note that the MappingOperation will be applied first.
   * This satisfies the StreamingOperation trait.
   */
  def compose(mappingOp: MapOperation): GroupingOperation = new GroupingOperation {
    //TODO: apply to metadata

    def aggregation: Aggregation = self.aggregation

    def groupByFunction(model: DataType): Sample => Option[DomainData] =
      mappingOp.mapFunction(model).andThen(self.groupByFunction(mappingOp.applyToModel(model)))

    override def applyToModel(model: DataType): DataType =
      self.applyToModel(mappingOp.applyToModel(model))
  }

  /**
   * Override to construct the new SampledFunction by
   * delegating to the original SampledFunction's
   * groupBy method.
   */
  override def applyToData(data: Data, model: DataType): Data = {
    val scalars = applyToModel(model) match {
      case Function(d, _) => d.getScalars
      case _ => throw LatisException(s"Invalid Dataset for grouping: $model")
    }
    val ord = LatisOrdering.partialToTotal(LatisOrdering.domainOrdering(scalars))
    data.asFunction.groupBy(groupByFunction(model), aggregation)(ord)
    //TODO: try groupBy with PartialOrdering, separate bin for the unorderable (e.g. NaN), then drop?
  }

}

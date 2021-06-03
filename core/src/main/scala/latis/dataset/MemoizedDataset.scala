package latis.dataset

import latis.data._
import latis.metadata.Metadata
import latis.model.DataType
import latis.ops.UnaryOperation
import latis.util.Identifier

/**
 * Defines a Dataset whose data is not connected
 * to an external data resource.
 */
class MemoizedDataset(
  _metadata: Metadata,
  _model: DataType,
  _data: MemoizedFunction,
  operations: Seq[UnaryOperation] = Seq.empty
) extends TappedDataset(_metadata, _model, _data, operations) with Serializable {

  /**
   * Returns a copy of this Dataset with the given Operation
   * appended to its sequence of operations.
   */
  override def withOperation(operation: UnaryOperation): Dataset =
    new MemoizedDataset(
      _metadata,
      _model,
      _data,
      operations :+ operation
    )

  override def rename(newId: Identifier): Dataset =
    new MemoizedDataset(
      _metadata + ("id" -> newId.asString),
      _model,
      _data,
      operations
    )

  /**
   * Returns the data as a MemoizedFunction with operations applied.
   */
  override def data: MemoizedFunction =
    applyOperations().map {
      case mf: MemoizedFunction => mf
      case sf: SampledFunction  => sf.unsafeForce
      case  d: Data => SeqFunction(
        List(Sample(DomainData(), RangeData(d)))
      )
    }.fold(throw _, identity)

}

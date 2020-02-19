package latis.dataset

import latis.data.MemoizedFunction
import latis.metadata.Metadata
import latis.model.DataType
import latis.ops.UnaryOperation

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
   * Returns the data as a MemoizedFunction.
   */
  override def data: MemoizedFunction = _data

}

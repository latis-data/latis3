package latis.dataset

import latis.metadata.Metadata
import latis.model.DataType
import latis.data.MemoizedFunction
import latis.ops.UnaryOperation

/**
 * Defines a Dataset whose data is not connected 
 * to an external data resource. 
 */
class MemoizedDataset(
  metadata: Metadata,
  model: DataType,
  data: MemoizedFunction,
  operations: Seq[UnaryOperation] = Seq.empty
) extends TappedDataset(metadata, model, data, operations) {
  
}
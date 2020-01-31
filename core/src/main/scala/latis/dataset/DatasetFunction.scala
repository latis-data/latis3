package latis.dataset

import latis.data.DomainData
import latis.data.RangeData
import latis.data.TupleData
import latis.metadata.Metadata
import latis.model.DataType
import latis.util.LatisException

//TODO: ContinuousDataset?
case class DatasetFunction(
  metadata: Metadata,
  model: DataType,
  //function: DomainData => Either[LatisException, RangeData]
  function: TupleData => Either[LatisException, TupleData]
) {

  /*
  TODO: can function domain be any List[Data]?
    e.g. spectrum => (r, g, b) needs to allow Function
    also no need for ordering
    another call for TupleData?
      then we could have Data => Data
   */

  def apply(data: TupleData): Either[LatisException, TupleData] =
    function(data)
}

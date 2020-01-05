package latis.dataset

import latis.data.DomainData
import latis.data.RangeData
import latis.metadata.Metadata
import latis.model.DataType
import latis.util.LatisException

case class DatasetFunction(
  metadata: Metadata,
  model: DataType,
  function: DomainData => Either[LatisException, RangeData]
) {

  def apply(data: DomainData): Either[LatisException, RangeData] =
    function(data)
}

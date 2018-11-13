package latis.ops

import latis.model.Dataset

trait BinaryOperation extends Operation {
  
  def apply(ds1: Dataset, ds2: Dataset): Dataset
  //TODO: provide default impl, delegate for metadata, model, data
  //TODO: enforce inclusion of prov metadata
}
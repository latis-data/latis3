package latis.data

import latis.model.DataType

trait DomainSet {
  //TODO: need model, no longer just data
  def model: DataType
  
  def elements: Seq[DomainData]
  
  def length: Int = elements.length
  
  //getShape: Array[Int]
  
  //TODO: interpolate?, insertion point
  def indexOf(data: DomainData): Int = 
    elements.indexOf(data) //TODO will this work or do we need indexWhere?
}

object DomainSet {
  
  def apply(_elements: Seq[DomainData], _model: DataType) = new DomainSet {
    def model = _model
    def elements = _elements
  }
}
package latis.data

import latis.metadata._
import latis.model._

trait DomainSet {
  //TODO: allow rank = 0? 

  def model: DataType =  {
    val scalars = (0 until rank) map { i => 
      val id = "_" + (i + 1)
      Scalar(Metadata("id" -> id, "type" -> "double"))
    }
    if (rank > 1) Tuple(scalars: _*)
    else scalars.head
  }
  
  def elements: Seq[DomainData]
  
  def length: Int = elements.length
  
  //number of dimensions, default to 1D
  //TODO: do we really want a default?
  def rank: Int = shape.length
  
  def shape: Array[Int] = Array(length) //default to 1D
  //TODO: consider Cartesian vs other topologies: e.g. rank 3, shape size 2
  
  def min: DomainData = getElement(0).get
  def max: DomainData = getElement(length - 1).get
  //TODO: first, last, take,...?
  
  def covers(data: DomainData): Boolean = 
    DomainOrdering.gteq(data, min) && DomainOrdering.lt(data, max)
   //TODO:  data > min && data < max
  
  def getElement(index: Int): Option[DomainData] = Option(elements(index))
  
  def indexOf(data: DomainData): Int = 
    elements.indexOf(data) //TODO will this work or do we need indexWhere?
    
  def map(f: DomainData => DomainData): DomainSet =
    DomainSet(elements.map(f))
}

object DomainSet {
  
  def apply(_elements: Seq[DomainData]) = new DomainSet {
    def elements = _elements
  }
}
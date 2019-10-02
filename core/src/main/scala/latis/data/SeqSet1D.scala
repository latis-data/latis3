package latis.data

import latis.model.DataType

/**
 * Define a one-dimensional DomainSet in term of a sequence of 
 * indexable DomainData elements.
 */
case class SeqSet1D(model: DataType, _elements: IndexedSeq[DomainData]) extends DomainSet {
  
  def elements: IndexedSeq[DomainData] = _elements

}

package latis.data

trait DomainSet {
  
  def elements: Seq[DomainData]
}

object DomainSet {
  
  def apply(_elements: Seq[DomainData]) = new DomainSet {
    def elements: Seq[DomainData] = _elements
  }
}
package latis.data

trait DomainSet {
  
  def elements: Seq[DomainData]
  
  def length: Int = elements.length
  
  //getShape: Array[Int]
  
  //TODO: interpolate?, insertion point
  def indexOf(data: DomainData): Int = 
    elements.indexOf(data) //TODO will this work or do we need indexWhere?
}

object DomainSet {
  
  def apply(_elements: Seq[DomainData]) = new DomainSet {
    def elements: Seq[DomainData] = _elements
  }
}
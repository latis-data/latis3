package latis.data

import scala.collection.Searching._

/**
 * Provide a base trait for Cartesian IndexedFunctions of various dimensions.
 */
trait IndexedFunction extends MemoizedFunction {
    
  def searchDomain(values: Seq[OrderedData], data: OrderedData): SearchResult = (values.head, data) match {
    case (_: Number, data: Number) => 
      values.asInstanceOf[Seq[Number]].search(data) 
    case (_: Text, data: Text) => 
      values.asInstanceOf[Seq[Text]].search(data)
    case _ => ??? //TODO error, invalid types
  }

}

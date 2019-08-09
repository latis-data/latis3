package latis.util

import scala.collection.immutable.SortedMap
import scala.collection.mutable.Builder

class CartesianMap1D[A: Ordering, V](map: SortedMap[A,V]) {
  //TODO: consider extending SortedMap, wait for 2.13 migration
  
  def apply(a: A): V = map(a)
  
  def get(a: A): Option[V] = map.get(a)
  
  def iterator: Iterator[(A,V)] = map.iterator
  
}

object CartesianMap1D {
  
  def apply[A: Ordering, V](elems: (A,V)*) =
    new CartesianMap1D(SortedMap(elems: _*))
  
//  def apply[A: Ordering, V](as: Seq[A], vs: Seq[V]) =
//    //TODO: assert same size
//    new CartesianMap1D(SortedMap(as zip vs: _*))
  
  def fromSeq[A: Ordering, V](ps: Seq[(A,V)]) = 
    new CartesianMap1D(SortedMap(ps: _*))
  
  def newBuilder[A: Ordering, V] = new Builder[(A, V), CartesianMap1D[A, V]] {
    val smBuilder: Builder[(A, V), SortedMap[A, V]] = SortedMap.newBuilder
    
    def +=(elem: (A,V)): this.type = {
      smBuilder += elem
      this
    }
    
    def clear(): Unit = smBuilder.clear()
    
    def result(): CartesianMap1D[A, V] = new CartesianMap1D(smBuilder.result())
  }
}

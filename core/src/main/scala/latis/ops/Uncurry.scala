package latis.ops

import latis.model._
import latis.metadata._
import latis.data._
import scala.collection.mutable.ArrayBuffer
import fs2._

/**
 * Given a Dataset with potentially nested Functions, undo the nesting.
 * For example:
 *   a -> b -> c  =>  (a, b) -> c
 *   
 * Assumes Cartesian Product domain set (e.g. same set of bs for each a).
 */
case class Uncurry() extends FlatMapOperation {
    //TODO: assume no Functions in Tuples, for now
    //TODO: neglect function and tuple IDs, for now 
  
  override def applyToModel(model: DataType): DataType = { 
    
    // Define buffers to accumulate domain and range types.
    val ds = ArrayBuffer[DataType]()
    val rs = ArrayBuffer[DataType]()
    
    // Recursive helper function to restructure the model
    def go(dt: DataType): Unit = dt match {
      case dt: Scalar => rs += dt
      case Tuple(es @ _*) => es.foreach(go)
      case Function(d, r) =>
        //flatten tuples, instead of recursing on domain (no nested functions)
        d match {
          case Tuple(es @ _*) => ds ++= es
          case s: Scalar => ds += d 
        }
        go(r)
    }

    // Start recursing into the model
    go(model)
    
    // Reconstruct the model from the new domain and range types
    val rtype = rs.length match {
      case 1 => rs.head
      case _ => Tuple(rs: _*)
    }
    ds.length match {
      case 0 => rtype
      case 1 => Function(ds.head, rtype)
      case _ => Function(Tuple(ds: _*), rtype) //TODO: flatten domain, Traversable builder not working
    }
  }
  

  /**
   * Define a function that takes a Sample potentially with nested Functions
   * and flattens the nested function by repeating the domain values for each 
   * inner Function Sample. Since we end up with a sequence of Sample for each
   * Sample (if there is a nested Function) we can wrap them as a SampledFunction.
   * Since we are going from Sample to SampledFunction, we can flatMap to a 
   * single uncurried/flattened SampledFunction in the end.
   * Note that this promises a MemoizedFunction since we already know that the 
   * entire Sample, including nested Function, is in memory.
   * 
   * This currently assumes no more than one layer of nested and that nested
   * Functions are not within a Tuple.
   */
  def flatMapFunction: Sample => MemoizedFunction = {
    (s: Sample) => s match {
      case Sample(ds, rs) => 
        //TODO: recurse for deeper nested functions
        //TODO: allow function in tuple
        rs.head match { //assumes only one type in range
          case sf: MemoizedFunction => sf map {
            case Sample(ds2, rs2) => Sample(ds ++ ds2, rs2)
          }
          case _ => SampledFunction(s) //no-op if range is not a Function
        }
    }
  }

}

package latis.ops

import scala.collection.mutable.ArrayBuffer

import latis.data._
import latis.model._
import latis.util.LatisException

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

  override def applyToModel(model: DataType): Either[LatisException, DataType] = {

    // Define buffers to accumulate domain and range types.
    val ds = ArrayBuffer[DataType]()
    val rs = ArrayBuffer[DataType]()

    // Recursive helper function to restructure the model
    def go(dt: DataType): Unit = dt match {
      case dt: Scalar     => rs += dt
      case t: Tuple       => t.elements.foreach(go)
      case Function(d, r) =>
        //flatten tuples, instead of recursing on domain (no nested functions)
        d match {
          case _: Scalar => ds += d
          case t: Tuple  => ds ++= t.elements
          case _: Function => ??? //Function not allowed in domain
        }
        go(r)
    }

    // Start recursing into the model
    go(model)

    // Reconstruct the model from the new domain and range types
    val rtype = rs.length match {
      case 1 => rs.head
      case _ => Tuple.fromSeq(rs.toSeq).fold(throw _, identity)
    }
    Right(ds.length match {
      case 0 => rtype
      case 1 => Function.from(ds.head, rtype).fold(throw _, identity)
      case _ => Function.from(Tuple.fromSeq(ds.toSeq).fold(throw _, identity), rtype).fold(throw _, identity)
      //TODO: flatten domain, Traversable builder not working
    })
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
   * This currently assumes no more than one layer of nesting and that nested
   * Functions are not within a Tuple.
   */
  def flatMapFunction(model: DataType): Sample => MemoizedFunction = {
    (sample: Sample) =>
      //TODO: recurse for deeper nested functions
      //TODO: allow function in tuple
      sample.range match {
        case ((sf: MemoizedFunction) :: Nil) => //one function in range
          val samples = sf.sampleSeq.map { s =>
            Sample(sample.domain ++ s.domain, s.range)
          }
          SampledFunction(samples)
        case _ => SampledFunction(Seq(sample)) //no-op if range is not a Function
      }
  }

}

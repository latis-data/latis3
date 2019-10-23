package latis.dataset

import latis.model.DataType
import latis.metadata.Metadata
import fs2.Stream
import latis.data.Sample
import cats.effect.IO
import latis.ops.UnaryOperation
import latis.input.Adapter
import java.net.URI

trait Dataset {
  
  def metadata: Metadata
  
  def model: DataType
  
  def samples: Stream[IO, Sample]
  
  def applyOperation(op: UnaryOperation): Dataset
  
}


case class AdaptedDataset(
  metadata: Metadata,
  model: DataType,
  ops: Seq[UnaryOperation],
  adapter: Adapter,
  uri: URI
) extends Dataset {
  
  def applyOperation(op: UnaryOperation): Dataset =
    this.copy(ops = ops :+ op)
    
  def samples: Stream[IO, Sample] = {
    //TODO: apply ops
    adapter(uri).streamSamples
  }
  
/*
 * TODO: make Dataset a trait
 * it should provide:
 *   metadata: Metadata
 *   model: DataType
 *   samples: fs2.Stream[Sample]
 *   apply operation: add it to the list, return new Dataset
 *   functional algebra DSL
 *   fromX DSL
 *   writeX DSL
 * encapsulate:
 *   Seq[Operation]
 *   SampledFunction or Adapter
 *   
 * move to latis package? latis.dataset?
 * 
 * Dataset has the scope to orchestrate (compile, apply) the operations
 * Adapters bring data into a SF, possibly handling some ops before reading
 *   often lazy as a StreamFunction
 * SFs apply ops immutably to make new SF
 *   
 */
  /* from HapiFunction
   * 
   * need to apply logic to set of operation, e.g. time min=max needs epsilon
   * save query building to last
   * but what do we accumulate?
   *   don't have the operation
   *   map?
   * should operations accumulate in the Dataset?
   *   request for model can be computed
   *   but what about data?
   *     only need to apply ops when we request samples
   *     wouldn't be safe to let a SF free
   *     should operations accumulate in SF?
   *       feels better
   *       but only the applyToData part
   *       encapsulate as DataOperation?
   *     could safely encapsulate SF and Ops in Dataset
   *       
   * Dataset as trait?
   * provide:
   *   metadata
   *   model
   *   samples
   *   functional algebra manipulate
   * encapsulates SF and Ops
   * requesting samples (e.g. to write) would trigger compilation/application of ops
   * some class of operations (think spark narrow vs wide transformations) break laziness
   * 
   * HapiDataset?
   * do we really need to push so much down to the SF level?
   * AdaptedDatasetSource can provide operations (PIs)
   * do we still need a HapiFunction?
   *   pushed down ops would need to happen first
   *   what would they operate on?
   *   should the adapter build the query to produce the first SF?
   *   more like v2
   *   would need adapter around later in the life cycle
   *     keep in mind need to reuse adapter for granules
   * Does DatasetSource still make sense?
   *   FdmlDataset?
   *   AdaptedDataset with adapter?
   *   but Reader semantics are nice
   *     FdmlReader could return AdaptedDataset
   * 
   * capture Pure vs IO
   *   AdaptedDataset would be in IO
   *   forcing it to memoize would make a pure dataset
   *   
   *   
   * keep RDD use case in mind: makes sense to keep data in RDD as a SF
   *   akin to using a nD array or other data structure to memoize in
   *   adapters are different: access to data outside of our control
   * 
   * FA + Ops + SF
   * project(expr: String) vs (ss: Seq[String]) vs (p: Projection)?
   * encapsulate in safe Projection
   * then might as well just apply(op: Operation)
   *   akin to our v2 handleOperation
   *   canHandleOperation(op) might be handy for compiler
   *   
   * use user friendly forms for Dataset, DSL
   */

}
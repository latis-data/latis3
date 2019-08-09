package latis.ops

import latis.data._
import latis.model._

case class GroupByBin(domainSet: DomainSet, agg: Aggregation) extends UnaryOperation {
  //agg: AggregatorFactory, make Aggregator for given DomainData
  
  /*
   * Can't stream, have to go through all orig samples to put in bins
   *   can't be MapOp
   * maybe some special cases where ordering allows bins to finish early
   * 
   * combine with Aggregation: SF => ConstantFunction of arity 0
   * 
   */
  
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    // Define a collection of DomainSet bins (Aggregators) to aggregate Samples
    val bins: IndexedSeq[Aggregator] = 
      domainSet.elements.toIndexedSeq.map(agg.makeAggregator(_))
    
      // Define function to put a given Sample into the appropriate bin
    def addSampleToBin(sample: Sample): Unit = {
      val index = domainSet.indexOf(sample.domain)
      if (index >= 0 && index < domainSet.length) bins(index).addOne(sample)
      //else out of bounds so ignore
    }
    
    // Consume the samples from the original SF and put them in the bins of the new DomainSet.
    // Note that Stream does not have foreach but we can map and ignore the return values with drain.
    data.streamSamples.map(addSampleToBin).compile.drain.unsafeRunSync
    
    // Get the new range data from the Aggregator for each bin
    val range: Seq[RangeData] = bins.map(_.result).map(_.getOrElse {
      // Use the resulting model to generate fill values
      applyToModel(model) match {
        case Function(_, r) => r.makeFillValues
      }
    })
    
    SetFunction(domainSet, range)
  }
  
  /*
   * GBB w/o agg: a -> b, d  =>  d -> a' -> b'
   * agg: a -> b  =>  c, where c can be a ConstantFunction but type should be c, CF is a data thing to make the var c look like a SF
   * a -> b, d  =>  d -> c
   */
  override def applyToModel(model: DataType): DataType = {
    Function(domainSet.model, agg.applyToModel(model))
  }
    //TODO: do we just need the range from the agg? ConstantFunction
    
  //TODO: update md
    
}

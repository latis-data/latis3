package latis.resample

import latis.data._
import scala.collection.mutable.Buffer
import scala.collection.mutable.Builder

/**
 * Define a resampling strategy that streams all the Samples
 * of a SampledFunction into the bins (i.e. cells) of a target
 * DomainSet and reduces the Samples in each bin to a single Sample
 * using nearest neighbor.
 */
case class BinResampling() extends Resampling {
  //TODO: make this an Operation, Binner + BinAggregator?
  //TODO: construct with any aggregator: mean, min, max, ...
  //TODO: csx: need type for new domain
  /*
   * TODO: can we replicate this with groupBy?
   * groupBy uses actual value
   * GroupByBin? 
   *   domainSet provide indexOf and new domain values
   *   assume same type
   *   agg and csx could also change model
   * default to SF if no agg
   * how do we generalize agg if NN needs center?
   *   presumably all need DomainData for each bin
   *   AggregatorFactory
   *   object that will return an instance given a DomainData
   *   
   * Aggregator as Operation
   * reduce set of Samples (equiv to SF) to a single sample
   * may change model
   * but don't generally produce a dataset
   *   e.g. this needs agg for each cell
   *   want shortcuts like NN using Builder
   * Aggregation as Operation with Aggregator?
   * apply to outer vs nested vs named Function?
   *   does it ever make sense to apply to outer?
   *   apply to range?
   * make Range data that gets assigned to the domainSet domain
   * 
   * can we bake in the csx? or assume it has been applied?
   * want to avoid cost of Substitution
   * compose MappingOperations by composing their mapping functions
   * ComposedOperation
   * but can this be a MapOperation?
   *   maybe a FlatMapOperation
   *   should be able to compose with it?
   * *could FlatMapOp + Aggregator = MapOp?
   *    akin to spark gruopBy making a thing that needs agg to make it an RDD again
   *    would have to reduce a SF to a RangeData
   *    reduce to single sample then drop the DomainData?
   *  but can't do GB with flatMap
   *    can we still compose?
   *    withCSX? implicit CSX?
   *    
   * trying to avoid cost of substitution
   * requires shuffling in spark since we partition by domain/key
   * mix csx into orig dataset with withCSX?
   * subst makes since, just a problem for spark
   *   though it does use more memory if memoized
   *   in general it works fine with stream
   *   it's when it is already memoized (e.g. RDD) that it matters
   *   add withCSX to be applied when samples are accessed
   *   is this really any diff for what would happen with separate ops?
   *     e.g. is samples.map(f) eager when samples is a Seq?
   *     ss.map(f).map(g) will do first eagerly unless its a Stream, even a scala.Stream
   *     presumably likewise for RDD, except for need to reshuffle?
   *     RddFunction does: 
   *       rdd.groupBy(groupByFunction)   groupByFunction: Sample => DomainData; returns RDD[(DD,Iterable[S])]
              .map(p => agg(p._1, p._2))  agg: (DomainData, Iterable[Sample]) => Sample; returns Sample with given domain
            should we have imlpicit conversion from SF to Iterable[S]?
           //TODO: look into PairRDDFunctions.aggregateByKey or PairRDDFunctions.reduceByKey
   *          these don't provide a way to generate a new domain
   *         
   *   Can we make a ComposedOperation with a MapOp and any Op?
   *   
   */
  
  def resample(sf: SampledFunction, domainSet: DomainSet, csx: DomainData => DomainData = identity): SampledFunction = {
    ???
//    
//    // Define a collection of DomainSet bins to aggregate Samples
//    val bins: IndexedSeq[BinAggregator] = 
//      domainSet.elements.map(NearestNeighborAggregator(_)).toIndexedSeq
//
//    // Define function to put a given Sample into the appropriate bin
//    def addSampleToBin(sample: Sample): Unit = {
//      val index = domainSet.indexOf(csx(sample.domain))
//      if (index >= 0 && index < domainSet.length) bins(index).addOne(sample)
//      //else out of bounds so ignore
//    }
//      
//    // Consume the samples from the original SF and put them in the bins of the new DomainSet
//    // Note that Stream does not have foreach but we can map and ignore the return values with drain.
//    sf.streamSamples.map(addSampleToBin).compile.drain.unsafeRunSync
//    
//    // Compute the new range data for each bin
//    val range: Seq[RangeData] = bins.map(_.result) map { maybeRange =>
//      maybeRange.getOrElse {
//        //RangeData(Double.NaN)
// RangeData(Double.NaN, Double.NaN, Double.NaN) //for use after image pivot
// //??? //TODO: replace fill value, need model
// // can we simply use RangeData.empty?
// // would actually prefer to get a NN here
//      }
//    }
//    
//    SetFunction(domainSet, range)
  }
}




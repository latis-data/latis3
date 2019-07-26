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
  //TODO: construct with any aggregator: mean, min, max, ...
  
  def resample(sf: SampledFunction, domainSet: DomainSet): SampledFunction = {
    
    // Define a collection of DomainSet bins to aggregate Samples
    val bins: IndexedSeq[BinAggregator] = 
      domainSet.elements.map(NearestNeighborAggregator(_)).toIndexedSeq

    // Define function to put a given Sample into the appropriate bin
    def addSampleToBin(sample: Sample): Unit = {
      val index = domainSet.indexOf(sample.domain)
      if (index >= 0 && index < domainSet.length) bins(index).addOne(sample)
      //else out of bounds so ignore
    }
      
    // Consume the samples from the original SF and put them in the bins of the new DomainSet
    // Note that Stream does not have foreach but we can map and ignore the return values with drain.
    sf.streamSamples.map(addSampleToBin).compile.drain.unsafeRunSync
    
    // Compute the new range data for each bin
    val range: Seq[RangeData] = bins.map(_.result).map(_.map(_.range)) map { maybeRange =>
      maybeRange.getOrElse {
        RangeData(Double.NaN)
        //??? //TODO: replace fill value
      }
    }
    
    SetFunction(domainSet, range)
  }
}


abstract class BinAggregator extends Builder[Sample, Option[Sample]] {
  //TODO: consider using fold (FP) instead of Builder (OO)

  protected var samples: Seq[Sample] = Seq.empty

  def addOne(sample: Sample): this.type
  
  def +=(sample: Sample) = addOne(sample)

  def clear() = samples = Seq.empty

  def result(): Option[Sample] = samples.headOption
}

/*
 * use one aggregator per bin
 * build it with the center as DomainData
 */
case class NearestNeighborAggregator(center: DomainData) extends BinAggregator {

  def addOne(sample: Sample): this.type = {
    samples.headOption match {
      case Some(currentSample) => 
        val nearestSample = Seq(currentSample, sample).minBy {
          case Sample(dd, _) => DomainData.distance(center, dd)
        }
        samples = Seq(nearestSample)
      case None => samples = Seq(sample)
    }
    this
  }
}

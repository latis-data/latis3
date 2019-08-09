package latis.ops

import latis.data._
import latis.model.DataType
import scala.collection.mutable.Builder

/**
 * Reduce a Function to a 0-arity (constant) function.
 * Note, the type of a ConstantFunction should simply be its range type.
 */
trait Aggregation extends UnaryOperation {
  def makeAggregator(domain: DomainData): Aggregator
}

//class AggregationFactory {
//  def apply(domain: DomainData): Aggregation
//}

case class NoAggregation() extends Aggregation //TODO: don't add prov

case class NearestNeighborAggregation() extends Aggregation {
  /*
   * can we simply wrap an aggregator? just the data part
   * how should an aggregation work outside of GroupByBin?
   *   need DomainData to make aggregator
   *   keep orig domain?
   *     a -> b  =>  a -> a -> b, only one sample in each nested function, nothing to aggregate, should this keep only the range?
   *   only interesting for nested?
   *     a -> b -> c  =>  a -> d where b -> c gets reduced to d
   *   how to specify which layer to agg? a->b->c->d
   *     do the range of the outer F? RangeOp
   *     if so, a and b need to be the same type or have a CSX
   *     
   * If Agg reduces the entire dataset to a const
   *   how do we get DomainData for aggregator?
   *   should it not be a general Op?
   * if this just does the range, what does it mean for GroupBy with agg?
   *   GB makes a new domain (GBB is given one)
   *   a -> b  =>  d -> a -> b
   *   so we do just need to agg the range
   *   by combining agg with BG we can avoid shuffling so much data?
   * 
   */
  
}

//TODO: mean, min, max,...

/**
 * Use a Builder to aggregate Samples into a single RangeData.
 */
abstract class Aggregator extends Builder[Sample, Option[RangeData]] {
  //TODO: consider using fold (FP) instead of Builder (OO)

  /**
   *  Manage the Samples that are being aggregated.
   */
  protected var samples: Seq[Sample] = Seq.empty

  /**
   * Add a Sample to this aggregation.
   */
  def addOne(sample: Sample): this.type
  
  /**
   * Add a Sample to this aggregation.
   * Alias for addOne.
   */
  def +=(sample: Sample) = addOne(sample)

  /**
   * Clear the Builder.
   */
  def clear() = samples = Seq.empty

  /**
   * Produce the final aggregated result:
   * the aggregation of Samples reduced to
   * maybe one RangeData.
   */
  def result(): Option[RangeData]
}

/*
 * use one aggregator per bin
 * build it with the center as DomainData
 */
case class NearestNeighborAggregator(center: DomainData) extends Aggregator {

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
  
  def result(): Option[RangeData] = samples.headOption.map(_.range)
}

object NearestNeighborAggregator extends AggregatorFactory {
  def apply(domain: DomainData) = NearestNeighborAggregator(domain)
}

//object NoAggregator extends AggregatorFactory {}


package latis.ops

import latis.data._
import latis.model.DataType
import scala.collection.mutable.Builder
import latis.model.Dataset
import scala.collection.mutable.ListBuffer
import fs2.Stream

/**
 * Reduce a Function to a 0-arity (constant) function.
 * Note, the type of a ConstantFunction should simply be its range type.
 */
trait Aggregation extends UnaryOperation {
  // Default to a 0-arity domain so we end up with a constant Function.
  def makeAggregator(domain: DomainData = DomainData()): Aggregator

  /**
   * Factor out the aggregation as a function that can be reused.
   */
  def aggregationFunction: (DomainData, Iterable[Sample]) => Sample =
    (dd: DomainData, samples: Iterable[Sample]) => {
      val agg = makeAggregator(dd) ++= samples //TODO: test performance when streaming orig samples into NN agg, lazy?
      val range = RangeData.fromSeq(agg.result())
      //TODO: use fill value if result is empty
      Sample(dd, range)
    }

  /*
   * TODO: depends on level of nesting, SamplePath
   * assume outer Function, for now
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    val dd = DomainData()
    val samples = data.unsafeForce.samples
    val range = aggregationFunction(dd, samples).range
    //TODO: just make a SF with the 0-arity DomainData?
    ConstantFunction(range)
  }
}

/**
 * Define a no-op Aggregation that keeps the Function as is.
 * This provides an Aggregator that combines Samples into a
 * SampledFunction.
 */
case class NoAggregation() extends Aggregation {

  // No-op
  override def apply(dataset: Dataset): Dataset = dataset

  /**
   * Although the NoAggregation Operation will be a no-op for a Dataset,
   * in some cases (e.g. GroupByBin) we want to use the Aggregator via
   * composition with this Aggregation. (Note that GBB also needs the model 
   * update for general aggregation.) The Aggregator here will simply 
   * collect the Samples added to it and build a SampledFunction from them.
   */
  def makeAggregator(domain: DomainData = DomainData()): Aggregator = new Aggregator {
    private val buffer: ListBuffer[Sample] = ListBuffer()
    def +=(sample: Sample) = { buffer += sample; this }
    def result(): Vector[Data] =
      if (buffer.length > 0) Vector(SampledFunction.fromSeq(buffer))
      else Vector.empty
  }
  
  /*
   * TODO:
   * should an aggregator in general provide RangeData?
   * or SF?
   * what if we want the average time sample from a time series
   *   it could produce an average time to use as the new domain
   *   but it would still be one sample, not much of a Function
   *   it could put the avg time in the range
   * 
   * what about agging a F within a tuple? wouldn't want RangeData
   *   t -> (a, w -> (f, u))
   *   just Data but could be a Seq[Data]
   *   
   */
}

//TODO: mean, min, max,... integration
  
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
   * There are uses cases for aggregating the outer F to a CF: e.g. integration
   *   CF needs no domain
   *   what does this mean for aggregator needing DD? just give it empty DD()? default?
   *   RangeAggregations vs Aggregation? 
   *     smart constructor could mixin RangeOperation
   *     but impl would have to op on range only
   *   or provide varID to indicate which?
   *     could agg at any nesting level
   *     use SamplePath!
   *     
   * Note, could use Aggregator as SampledFunction builder
   * though wrapped in a RangeData
   */
  


/**
 * Use a builder to aggregate Samples into a single collection of Data values.
 */
trait Aggregator {
  //TODO: consider using fold (FP) instead of builder (OO)
  
  /**
   * Add a Sample to this aggregation.
   */
  def +=(sample: Sample): this.type

  /**
   * Add a collection of Samples to this Aggregator.
   */
  def ++=(samples: Iterable[Sample]): this.type = {
    samples foreach +=
    this
  }
  
  /**
   * Produce the final aggregated result:
   * the aggregation of Samples reduced to a Vector of Data.
   * This returns a Vector so this can contain multiple Data
   * values such as members of a Tuple range of the aggregated
   * Samples. Not all cases can be reduced to RangeData (e.g.
   * a Function within a Tuple).
   */
  def result(): Vector[Data]
}



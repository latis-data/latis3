package latis.ops

import scala.collection.mutable.ListBuffer

import latis.data._
import latis.model.DataType

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
      val agg   = makeAggregator(dd) ++= samples //TODO: test performance when streaming orig samples into NN agg, lazy?
      val range = RangeData(agg.result())
      //TODO: use fill value if result is empty
      Sample(dd, range)
    }

  /*
   * TODO: depends on level of nesting, SamplePath
   * assume outer Function, for now
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    val dd      = DomainData()
    val samples = data.unsafeForce.samples
    val range   = aggregationFunction(dd, samples).range
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

  /**
   * Although the NoAggregation Operation will be a no-op for a Dataset,
   * in some cases (e.g. GroupByBin) we want to use the Aggregator via
   * composition with this Aggregation. (Note that GBB also needs the model
   * update for general aggregation.) The Aggregator here will simply
   * collect the Samples added to it and build a SampledFunction from them.
   */
  def makeAggregator(domain: DomainData = DomainData()): Aggregator = new Aggregator {
    private val buffer: ListBuffer[Sample] = ListBuffer()
    def +=(sample: Sample)                 = { buffer += sample; this }
    def result(): List[Data] =
      if (buffer.nonEmpty) List(SampledFunction(buffer))
      else List.empty
  }
}

//TODO: mean, min, max,... integration

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
    samples.foreach(+=)
    this
  }

  /**
   * Produce the final aggregated result:
   * the aggregation of Samples reduced to a List of Data.
   * This returns a List so this can contain multiple Data
   * values such as members of a Tuple range of the aggregated
   * Samples. Not all cases can be reduced to RangeData (e.g.
   * a Function within a Tuple).
   */
  def result(): List[Data]
}

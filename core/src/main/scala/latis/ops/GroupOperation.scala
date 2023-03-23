package latis.ops

import scala.collection.immutable.SortedMap

import cats.data.Chain
import cats.effect.IO
import fs2.Pipe
import fs2.Stream

import latis.data.*
import latis.model.DataType
import latis.model.Function
import latis.util.CartesianDomainOrdering
import latis.util.LatisException
import latis.util.LatisOrdering

/**
 * Defines a type of Operation that groups Samples
 * by applying a function that computes a new domain
 * for each Sample.
 */
trait GroupOperation extends StreamOperation { self =>

  /**
   * Defines the type of the new domain.
   */
  def domainType(model: DataType): DataType

  /**
   * Defines the ordering of the new domain data.
   */
  def ordering(model: DataType): Ordering[DomainData] =
    LatisOrdering.partialToTotal(
      CartesianDomainOrdering(domainType(model).getScalars.map(_.ordering))
    )

  /**
   * Defines an Aggregation Operation to use to reduce
   * a collection of Samples to a single RangeData.
   */
  def aggregation: Aggregation

  /**
   * Defines a function that creates a DomainData from a Sample.
   * This will return None if there is no place for the Sample
   * in the resulting Dataset.
   * The DomainData will be used to define the
   * domain set of the resulting Dataset.
   */
  def groupByFunction(model: DataType): Sample => Option[DomainData]

  ///**
  // * Composes this Operation with a MapOperation.
  // * Note that the MapOperation will be applied first.
  // */
  //def compose(mapOp: MapOperation): GroupOperation = new GroupOperation {
  //  //TODO: apply to metadata
  //
  //  def domainType: DataType  =
  //
  //  def ordering: Ordering[DomainData] = self.ordering
  //
  //  def groupByFunction: Sample => Option[DomainData] =
  //    mapOp.mapFunction.andThen(self.groupByFunction)
  //}

  /**
   * Constructs a SortedMap to use as a temporary data structure
   * to accumulate samples into groups.
   */
  def makeSortedMap(model: DataType): SortedMap[DomainData, Chain[Sample]] =
    SortedMap.empty[DomainData, Chain[Sample]](ordering(model))

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = in => {
    /*
     TODO: NNAgg also needs dd
     optional arg to aggregation.aggregateFunction?
     */
    val aggF = aggregation.aggregateFunction(model)
    val grpF = groupByFunction(model)

    in.fold(makeSortedMap(model)) { case (groups, sample) =>
      grpF(sample) match {
        case Some(dd) =>
          val ss = groups.getOrElse(dd, Chain.empty)
          groups + (dd -> (ss :+ sample))
        case None => groups
      }
    }.flatMap { groups =>
      Stream.emits(groups.toSeq).map {
        case (dd, ss) => Sample(dd, RangeData(aggF(ss.toList)))
      }
    }
  }

  /**
   * Overrides applyToModel to use the new domain type for the domain.
   * The original model becomes the range which is then modified by the
   * Aggregation.
   */
  override def applyToModel(model: DataType): Either[LatisException, DataType] =
    aggregation.applyToModel(model).map(Function.from(domainType(model), _).fold(throw _, identity))
    // TODO: preserve metadata from the original function
}

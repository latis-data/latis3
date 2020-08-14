package latis.ops

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import cats.effect.IO
import fs2.Pipe
import fs2.Stream

import latis.data._
import latis.model.DataType
import latis.model.Function
import latis.util.CartesianDomainOrdering
import latis.util.LatisOrdering
import latis.util.StreamUtils

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
  def makeSortedMap(model: DataType): mutable.SortedMap[DomainData, ListBuffer[Sample]] =
    mutable.SortedMap[DomainData, ListBuffer[Sample]]()(ordering(model))

  def pipe(model: DataType): Pipe[IO, Sample, Sample] = {
    val sortedMap = makeSortedMap(model)

    (stream: Stream[IO, Sample]) => {
      stream.map { sample =>
        // Group the Samples into the sortedMap
        groupByFunction(model)(sample) match {  //Option[DomainData]
          case Some(dd) => sortedMap.get(dd) match { //Option[ListBuffer]
            case Some(buffer) => buffer += sample; ()
            case None => sortedMap += (dd -> ListBuffer(sample)); ()
          }
          case None => //No valid DomainData found so drop Sample
        }
      }.compile.drain.unsafeRunSync()
      /*
      TODO: avoid unsafe run
        fold the stream?
        could GroupOp be a FoldOp?
       */

      val aggF: Iterable[Sample] => Data =
        aggregation.aggregateFunction(model)

      // For each buffer, aggregate the Samples to make a new Sample.
      StreamUtils.seqToIOStream(sortedMap.toSeq) map {
        case (dd, ss) =>
          Sample(dd, RangeData(aggF(ss)))
        /*
        TODO: NNAgg also needs dd
          optional arg to aggregation.aggregateFunction?
         */
      }
    }
  }

  /**
   * Overrides applyToModel to use the new domain type for the domain.
   * The original model becomes the range which is then modified by the
   * Aggregation.
   */
  override def applyToModel(model: DataType): DataType = {
    val range = aggregation.applyToModel(model)
    Function(domainType(model), range)
    // TODO: preserve metadata from the original function
  }
}

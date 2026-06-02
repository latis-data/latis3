package latis.ops

import scala.annotation.tailrec
import scala.collection.mutable

import cats.syntax.all.*
import cats.PartialOrder
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.CartesianDomainOrdering
import latis.util.Identifier
import latis.util.Identifier.*
import latis.util.LatisException

/**
 * Binary operation to combine datasets "horizontally".
 *
 * The resulting dataset will have the shared domain type and a
 * concatenation of the range variables.
 *
 * {{{x -> a  |+|  x -> b  =>  x -> (a, b)}}}
 *
 * The domain type of the first dataset will be used. The domain type
 * of the second dataset must be compatible (based on id, type, and units)
 * or an Index (which will be treated as equivalent to any sample it is
 * compared to). Any other domain variable properties in the second dataset
 * will not be preserved. The domain may have any number of dimensions, but
 * it must be Cartesian (to facilitate ordering). Raw Scalars and Tuples
 * (0-dimensional) are also supported.
 *
 * There is no restriction on the range data types since they will simply be
 * included in the new range. Range variables are treated as being distinct.
 * Duplicate identifiers will be disambiguated by appending a counter of
 * the form "_#" with the first being "_1".
 *
 * Ordering, including equivalency, is used to align the samples from each
 * dataset. Multidimensional Functions are assumed to be Cartesian. If one
 * dataset has a sample for a given domain value where the other doesn't,
 * fill values may be introduced depending on the HorizontalJoinType behavior:
 *
 *  - [[HorizontalJoinType.Full Full]] All samples are kept with fill values
 *
 *  - [[HorizontalJoinType.Left Left]] All samples from the first dataset are kept
 *      with fill values for the second
 *
 *  - [[HorizontalJoinType.Right Right]] All samples from the second dataset are kept
 *      with fill values for the first
 *
 *  - [[HorizontalJoinType.Inner Inner]] Only samples with equivalent domain values are kept
 */
class HorizontalJoin(joinType: HorizontalJoinType) extends Join {
  //TODO: validate that Function is Cartesian
  //TODO: consider chunk size
  //TODO: do we need to timeout? e.g. no more db connections deadlock

  /** Use first domain and concatenate ranges */
  override def applyToModel(model1: DataType, model2: DataType): Either[LatisException, DataType] = {
    if (comparableDomain(model1, model2)) {
      val range = Tuple.fromSeq(concatRange(model1, model2))
      model1 match {
        case Function(domain, _) => range.flatMap(r => Function.from(domain, r))
        case _ => range
      }
    } else LatisException("Join requires compatible domain").asLeft
  }

  /** Returns combined samples and the remainder of each chunk */
  override def joinChunks(
    model1: DataType,
    c1: Chunk[Sample],
    model2: DataType,
    c2: Chunk[Sample]
  ): (Chunk[Sample], Chunk[Sample], Chunk[Sample]) = {

    // Define a PartialOrder for domain data, assumes Cartesion
  //TODO: util from SortedJoin
    val ord = PartialOrder.fromPartialOrdering {
      model1 match {
        case Function(domain, _) =>
          CartesianDomainOrdering(domain.getScalars.map(_.ordering))
        case _ => CartesianDomainOrdering(List.empty) //always equivalent
      }
    }

    // Recursive function to accumulate joined samples.
    // Returns the combined samples and remainder.
    @tailrec
    def go(
      acc: Chunk[Sample],
      c1: Chunk[Sample],
      c2: Chunk[Sample]
    ): (Chunk[Sample], Chunk[Sample], Chunk[Sample]) = {
      // Accumulate samples recursively, filling as needed
      if (c1.nonEmpty && c2.nonEmpty) {
        val sample1 = c1.head.get
        val sample2 = c2.head.get
        if (
          domainIsIndex(model1) ||
          domainIsIndex(model2) ||
          ord.eqv(sample1.domain, sample2.domain)
        ) {
          // Equivalent domain value so combine range variables
          val chunk = Chunk(Sample(sample1.domain, sample1.range ++ sample2.range))
          go(acc ++ chunk, c1.drop(1), c2.drop(1))
        }
        else if (ord.lt(sample1.domain, sample2.domain)) {
          // Maybe fill on the right
          val chunk = fillRight(model2, Chunk(sample1)).getOrElse(Chunk.empty)
          go(acc ++ chunk, c1.drop(1), c2)
        } else if (ord.gt(sample1.domain, sample2.domain)) {
          // Maybe fill on the left
          val chunk = fillLeft(model1, Chunk(sample2)).getOrElse(Chunk.empty)
          go(acc ++ chunk, c1, c2.drop(1))
        }
        else (Chunk.empty, Chunk.empty, Chunk.empty) //invalid samples, domains not comparable
      } else {
        // If a chunk is empty, terminate so the caller will provide more samples
        (acc, c1, c2)
      }
    }

    // If both chunks are not empty, recursively join the samples.
    // If both are empty, there is nothing to join.
    // If one is empty, fill as needed.
    // Note, we can't do this test above because they may be empty while recursing.
    if (c1.isEmpty && c2.isEmpty) (Chunk.empty, Chunk.empty, Chunk.empty)
    else if (c1.isEmpty) {
      val chunk = fillLeft(model1, c2).getOrElse(Chunk.empty)
      (chunk, Chunk.empty, Chunk.empty)
    }
    else if (c2.isEmpty) {
      val chunk = fillRight(model2, c1).getOrElse(Chunk.empty)
      (chunk, Chunk.empty, Chunk.empty)
    }
    else go(Chunk.empty, c1, c2) //recursively join chunks
  }

//==== Utility methods ====//

  /** Concatenate the range variables, disambiguating ids appending "_#" */
  private def concatRange(model1: DataType, model2: DataType): List[DataType] = {
    val vars = model1.rangeVariables ++ model2.rangeVariables

    // Count number of occurrences of each variable.
    // This ignores any "_#" assumed to have come from a previous join.
    val idCount = vars.groupMapReduce(variableId)(_ => 1)(_ + _)

    // Keep track of the counters used
    val used = vars.foldLeft(mutable.Map[Identifier, Int]()) { (m, v) =>
      m += (variableId(v) -> 0)
    }

    // Append "_#" to disambiguate variable ids as needed
    vars.map { v =>
      val id = variableId(v)
      if (idCount(id) == 1) v //disambiguation not needed
      else {
        val n = used(id) + 1
        used.update(id, n)
        val id2 = Identifier.fromString(s"${id.asString}_$n").get
        v.rename(id2)
      }
    }
  }

  /**
   * Get an identifier for a variable (or generate one).
   * Drop any previous "_#" disambiguators, assuming they were added by a previous join.
   */
  //TODO: risk that variable may have an id with "_#" for legitimate reasons
 //TODO: disambiguate with dataset name like join service in latis-telemetry?
  private def variableId(variable: DataType): Identifier = variable match {
    case s: Scalar    => Identifier.fromString("_\\d+$".r.replaceAllIn(s.id.asString, "")).get
    case t: Tuple     => t.id.getOrElse(id"tup")
    case f: Function  => f.id.getOrElse(id"func")
  }

//TODO: improve wording
  /**
   * If the join type permits, make fill data for the first
   * dataset using the given model. Use the domains from a
   * chunk of samples from the second dataset and append its
   * range variables to the fill data.
   *
   * Note that this may result in NullData.
   */
  private def fillLeft(model: DataType, chunk: Chunk[Sample]): Option[Chunk[Sample]] = {
    if (
      domainIsIndex(model) ||
      this.joinType == HorizontalJoinType.Left ||
      this.joinType == HorizontalJoinType.Inner
    ) None
    else chunk.map { sample =>
      val range = model.rangeVariables.map(_.fillData) ++ sample.range
      Sample(sample.domain, range)
    }.some
  }

  /**
   * If the join type permits, make fill data for the second
   * dataset using the given model. Use the domains from a
   * chunk of samples from the first dataset and prepend its
   * range variables to the fill data.
   *
   * Note that this may result in NullData.
   */
  private def fillRight(model: DataType, chunk: Chunk[Sample]): Option[Chunk[Sample]] = {
    if (
      domainIsIndex(model) ||
      this.joinType == HorizontalJoinType.Right ||
      this.joinType == HorizontalJoinType.Inner
    ) None
    else chunk.map { sample =>
      val range = sample.range ++ model.rangeVariables.map(_.fillData)
      Sample(sample.domain, range)
    }.some
  }

  /** Is the given model a Function of Index */
  private def domainIsIndex(model: DataType): Boolean =
    model match {
      case Function(_: Index, _) => true
      case _ => false
    }

}

/** Enumeration of join types */
enum HorizontalJoinType:
  case Full, Left, Right, Inner

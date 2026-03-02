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
 * The resulting domain will be that of the first dataset (should be the
 * same or Index) and the resulting range will be a concatenation of the
 * range variables from each dataset. There is no restriction on the data
 * types. The join will only be applied to the outer Function.
 *
 * This expects that the domains of each dataset are comparable.
 * These could be 0-arity or include an Index which will always match.
 * If one has a sample for a given domain value where the other doesn't,
 * the HorizontalJoinType will specify the behavior:
 *
 *  - [[Full]] All samples are kept with fill values
 *  - [[Left]] All samples from the first dataset are kept
 *             with fill values for the second
 *  - [[Right]] All samples from the second dataset are kept
 *              with fill values for the first
 *  - [[Inner]] Only samples with equivalent domain values are kept
 */
class HorizontalJoin(joinType: HorizontalJoinType = HorizontalJoinType.Full) extends Join {
  //TODO: consider chunk size
  //TODO: do we need to timeout? e.g. no more db connections deadlock
  //  does that mean we need a big/elastic connection pool to join a lot of items?
  //TODO: Use Interpolation when fill is needed
  //  need previous, return leftovers for both chunks
  //  only need more for lesser of last element, but Join does not apply order
  //  risk if we get for both over a big gap in one?

  /** Use first domain and concatenate ranges */
  override def applyToModel(model1: DataType, model2: DataType): Either[LatisException, DataType] = {
    if (comparableDomain(model1, model2)) {
      val range = Tuple.fromSeq(concatRange(model1, model2))
      model1 match {
        case Function(domain, _) => range.flatMap(r => Function.from(domain, r))
        case _ => range
      }
    } else LatisException("Join requires same domain").asLeft
  }

  /** Returns a new chunk and the remainder of the other two */
  override def joinChunks(
    model1: DataType,
    c1: Chunk[Sample],
    model2: DataType,
    c2: Chunk[Sample]
  ): (Chunk[Sample], Chunk[Sample], Chunk[Sample]) = {

    // Define a PartialOrder for domain data
    val ord = PartialOrder.fromPartialOrdering {
      model1 match {
        case Function(domain, _) =>
          CartesianDomainOrdering(domain.getScalars.map(_.ordering))
        case _ => CartesianDomainOrdering(List.empty) //TODO: no domain, always eqv
      }
    }

    @tailrec
    def go(
      acc: Chunk[Sample],
      c1: Chunk[Sample],
      c2: Chunk[Sample]
    ): (Chunk[Sample], Chunk[Sample], Chunk[Sample]) = {
      if (c1.nonEmpty && c2.nonEmpty) {
        val sample1 = c1.head.get
        val sample2 = c2.head.get
        if (
          domainIsIndex(model1) ||
          domainIsIndex(model2) ||
          ord.eqv(sample1.domain, sample2.domain)
        ) {
          // Equivalent domain value so join range
          val chunk = Chunk(Sample(sample1.domain, sample1.range ++ sample2.range))
          go(acc ++ chunk, c1.drop(1), c2.drop(1))
        }
        else if (ord.lt(sample1.domain, sample2.domain)) {
          // Maybe fill on the right
          val chunk = fillRight(model2, Chunk(sample1)).getOrElse(Chunk.empty)
          go(acc ++ chunk, c1.drop(1), c2)
        } else if (ord.gt(sample1.domain, sample2.domain)) {
          // Maybe fill on the left
          val chunk = fillRight(model2, Chunk(sample1)).getOrElse(Chunk.empty)
          go(acc ++ chunk, c1, c2.drop(1))
        }
        else (Chunk.empty, Chunk.empty, Chunk.empty) //invalid samples, domains not comparable
      } else (acc, c1, c2)
    }

    // Handle empty chunks by filling or recursively join.
    // Note, we can't do this test above because they may be empty while recursing.
    if (c1.isEmpty && c2.isEmpty) (Chunk.empty, Chunk.empty, Chunk.empty)
    else if (c1.isEmpty) {
      val chunk = fillLeft(model1, c2).getOrElse(Chunk.empty)
      (chunk, Chunk.empty, Chunk.empty)
    }
    else if (c2.isEmpty) {
      val chunk = fillLeft(model2, c1).getOrElse(Chunk.empty)
      (chunk, Chunk.empty, Chunk.empty)
    }
    else go(Chunk.empty, c1, c2)
  }

//==== Utility methods ====//

  /** Concatenate the range variables, disambiguating ids appending "_#" */
  private def concatRange(model1: DataType, model2: DataType): List[DataType] = {
    val vars = model1.rangeVariables ++ model2.rangeVariables

    // Count number of occurrences of each variable ignoring previous "_#"
    val idCount = vars.groupMapReduce(variableId)(_ => 1)(_ + _)

    // Keep track of the counters used
    val used = vars.foldLeft(mutable.Map[Identifier, Int]()) { (m, v) =>
      m += (variableId(v) -> 0)
    }

    vars.map { v =>
      val id = variableId(v)
      if (idCount(id) == 1) v
      else {
        val n = used(id) + 1
        used.update(id, n)
        val id2 = Identifier.fromString(s"${id.asString}_$n").get
        v.rename(id2)
      }
    }
  }

  /**
   * Get a variable identifier (or generate one).
   * Drop any previous "_#" disambiguators, TODO: risky consequences?
   */
  //TODO: beef up handling of other types
  private def variableId(variable: DataType): Identifier = variable match {
    case s: Scalar    => Identifier.fromString("_\\d+$".r.replaceAllIn(s.id.asString, "")).get
    case t: Tuple     => t.id.getOrElse(id"tup")
    case f: Function  => f.id.getOrElse(id"func")
  }

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
private enum HorizontalJoinType:
  case Full, Left, Right, Inner

package latis.ops

import scala.annotation.tailrec

import cats.syntax.all.*
import cats.PartialOrder
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.CartesianDomainOrdering
import latis.util.LatisException

/**
 * Binary operation to combine datasets "horizontally".
 *
 * The resulting domain will remain unchanged and the range variables
 * from each dataset will be included in the resulting range.
 * This expects that the domains of each match (could be 0-arity)
 * and assumes the range variables are all different. If one has a
 * sample for a given domain value where the other doesn't, the
 * HorizontalJoinType will specify the behavior:
 *  - [[Full]] All samples are kept with fill values
 *  - [[Left]] All samples from the first dataset are kept
 *             with fill values for the second
 *  - [[Right]] All samples from the second dataset are kept
 *              with fill values for the first
 *  - [[Inner]] Only samples with the same domain values are kept
 */
class HorizontalJoin(joinType: HorizontalJoinType = HorizontalJoinType.Full) extends Join {
  //TODO: consider chunk size
  //TODO: do we need to timeout? e.g. no more db connections deadlock
  //  does that mean we need a big/elastic connection pool to join a lot of items?

  //TODO!!: deal with duplicate names
  //  e.g. all telemetry have dn, value
  //  will Tuple creation fail?
  //  prepend dataset name? but don't have access to dataset
  //  append _#?
  //  do via combine where we do have datasets?
  override def applyToModel(model1: DataType, model2: DataType): Either[LatisException, DataType] = {
    if (equivalentDomain(model1, model2)) {
      val range = Tuple.fromSeq(rangeVariables(model1) ++ rangeVariables(model2))
      model1 match {
        case Function(domain, _) => range.flatMap(r => Function.from(domain, r))
        case _ => range
      }
    } else LatisException("Join requires same domain").asLeft
  }

  // Returns a new chunk and the remainder of the other two
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
        if (ord.eqv(sample1.domain, sample2.domain)) {
          // Same domain so join range
          val s = Sample(sample1.domain, sample1.range ++ sample2.range)
          go(acc ++ Chunk(s), c1.drop(1), c2.drop(1))
        }
        else if (ord.lt(sample1.domain, sample2.domain)) {
          // Fill on the right if not a right join, may be NullData
          if (fillRight) go(acc ++ fillRight(model2, Chunk(sample1)), c1.drop(1), c2)
          else go(acc, c1.drop(1), c2)
        } else if (ord.gt(sample1.domain, sample2.domain)) {
          // Fill on the left if not a left join, may be NullData
          if (fillLeft) go(acc ++ fillLeft(model1, Chunk(sample2)), c1, c2.drop(1))
          else go(acc, c1, c2.drop(1))
        }
        else ??? //TODO: invalid samples, domains not comparable
      } else (acc, c1, c2)
    }

    // Handle empty chunks by filling or recursively join.
    // Note, we can't do this test above because they may be empty while recursing.
    if (c1.isEmpty && c2.isEmpty) (Chunk.empty, Chunk.empty, Chunk.empty)
    else if (c1.isEmpty) {
      if (fillLeft) (fillLeft(model1, c2), Chunk.empty, Chunk.empty)
      else (Chunk.empty, Chunk.empty, Chunk.empty)
    }
    else if (c2.isEmpty) {
      if (fillRight) (fillRight(model2, c1), Chunk.empty, Chunk.empty)
      else (Chunk.empty, Chunk.empty, Chunk.empty)
    }
    else go(Chunk.empty, c1, c2)
  }

  private def fillLeft(model: DataType, chunk: Chunk[Sample]): Chunk[Sample] = {
    chunk.map { sample =>
      val range = rangeVariables(model).map(_.fillData) ++ sample.range
      Sample(sample.domain, range)
    }
  }

  private def fillRight(model: DataType, chunk: Chunk[Sample]): Chunk[Sample] = {
    chunk.map { sample =>
      val range = sample.range ++ rangeVariables(model).map(_.fillData)
      Sample(sample.domain, range)
    }
  }

  /**
   * Gets the variables in the range of a model.
   *
   * All types of variables will be handled.
   * This also supports 0-arity models (e.g. Tuple or Scalar).
   */
  //TODO: util?
  private def rangeVariables(model: DataType): List[DataType] = model match {
    case Function(_, range) => range match {
      case Tuple(es *) => es.toList
      case v           => List(v)
    }
    case Tuple(es *) => es.toList //0-arity
    case s: Scalar   => List(s)   //0-arity
  }

  /** Should this join include missing samples in the first dataset */
  private def fillLeft: Boolean =
    this.joinType == HorizontalJoinType.Right ||
      this.joinType == HorizontalJoinType.Full

  /** Should this join include missing samples in the second dataset */
  private def fillRight: Boolean =
    this.joinType == HorizontalJoinType.Left ||
      this.joinType == HorizontalJoinType.Full
}

enum HorizontalJoinType:
  case Full, Left, Right, Inner

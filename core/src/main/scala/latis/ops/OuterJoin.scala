package latis.ops

import scala.annotation.tailrec

import cats.data.Chain
import cats.syntax.all.*
import cats.PartialOrder
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.CartesianDomainOrdering
import latis.util.LatisException

/**
 *
 *
 * An outer join keeps all samples from both datasets
 * may need fill
 *
 * May be 0-arity
 * Range can have any type.
 * Assumes all range vars are unique, would need to rename
 * Note that if there is the same var in both, then the user should project before join
 */
class OuterJoin extends Join2 { //TODO: need model to apply to data
  //TODO: consider chunk size
  //TODO: do we need to timeout? e.g. no more connections deadlock
  //  does that mean we need a big connection pool to join a lot of items?
  //  when is the connection released? after exhausting result set?
  //TODO: left and right outer join with boolean?


  //TODO!!: deal with duplicate names
  //  e.g. all telemetry have dn, value
  //  prepend dataset name? but don't have access to dataset
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
    //TODO: make sure model domains are the same

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
      acc: Chain[Sample],
      l1: List[Sample],
      l2: List[Sample]
    ): (Chain[Sample], List[Sample], List[Sample]) = {
      if (l1.isEmpty || l2.isEmpty) (acc, l1, l2) //done here
      else {
        val sample1 = l1.head
        val sample2 = l2.head
        if (ord.eqv(sample1.domain, sample2.domain)) { //same domain so join range
          val s = Sample(sample1.domain, sample1.range ++ sample2.range)
          go(acc :+ s, l1.tail, l2.tail)
        }
        else if (ord.lt(sample1.domain, sample2.domain)) {
          // Fill on the right, may be NullData
          val range = sample1.range ++ rangeVariables(model2).map(_.fillData)
          val sample = Sample(sample1.domain, range)
          go(acc :+ sample, l1.tail, l2)
        } else {
          // Fill on the left, may be NullData
          val range = rangeVariables(model1).map(_.fillData) ++ sample2.range
          val sample = Sample(sample2.domain, range)
          go(acc :+ sample, l1, l2.tail)
        }
      }
    }

    val (acc, r1, r2) = go(Chain.empty, c1.toList, c2.toList)
    (Chunk.chain(acc), Chunk.from(r1), Chunk.from(r2))
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

}

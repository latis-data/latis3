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
 * Binary operation to combine datasets with overlapping coverage.
 *
 * This expects that both datasets have the same model.
 * Samples from each dataset will be interleaved to preserve order.
 * If two samples have the same domain values, the sample from the
 * first dataset will be kept.
 */
class SortedJoin extends Join2 {
  //TODO: validate same model
  //TODO: consider chunk size
  //TODO: use generic sortedMerge from latis3-packets?
  //TODO: consider other tie breakers: keep second, average, ...
  //      use Interpolation?

  // Models must be the same and do not change.
  override def applyToModel(model1: DataType, model2: DataType): Either[LatisException, DataType] =
    model1.asRight

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
          // Same domain, keep left
          go(acc ++ Chunk(sample1), c1.drop(1), c2.drop(1))
        }
        else if (ord.lt(sample1.domain, sample2.domain)) {
          // Left comes first, keep it
          go(acc ++ Chunk(sample1), c1.drop(1), c2)
        } else if (ord.gt(sample1.domain, sample2.domain)) {
          // Right comes first, keep it
          go(acc ++ Chunk(sample2), c1, c2.drop(1))
        }
        else ??? //TODO: invalid samples, domains not comparable
      } else (acc, c1, c2)
    }

    if (c1.isEmpty && c2.isEmpty) (Chunk.empty, Chunk.empty, Chunk.empty)
    else if (c1.isEmpty) (c2, Chunk.empty, Chunk.empty)
    else if (c2.isEmpty) (c1, Chunk.empty, Chunk.empty)
    else go(Chunk.empty, c1, c2)
  }
}

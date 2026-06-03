package latis.ops

import scala.annotation.tailrec

import cats.PartialOrder
import fs2.*

import latis.data.*
import latis.model.*

/**
 * Vertical join to combine datasets with overlapping coverage.
 *
 * This expects that both datasets have the same (or compatible) model.
 * Samples from each dataset will be interleaved to preserve order.
 * If two samples have the same domain values, the sample from the
 * first dataset will be kept.
 */
class SortedJoin extends VerticalJoin {
  //TODO: use generic sortedMerge from latis3-packets?
  //TODO: consider other tie breakers: keep second, average,...

  override def joinChunks(
    model1: DataType,
    c1: Chunk[Sample],
    model2: DataType,
    c2: Chunk[Sample]
  ): (Chunk[Sample], Chunk[Sample], Chunk[Sample]) = {

    // Define a PartialOrder for samples, assumes Cartesion
    val ord: PartialOrder[Sample] = cartesianOrder(model1)

    @tailrec
    def go(
      acc: Chunk[Sample],
      c1: Chunk[Sample],
      c2: Chunk[Sample]
    ): (Chunk[Sample], Chunk[Sample], Chunk[Sample]) = {
      if (c1.nonEmpty && c2.nonEmpty) {
        val sample1 = c1.head.get //confirmed not empty
        val sample2 = c2.head.get //confirmed not empty
        if (ord.eqv(sample1, sample2)) {
          // Same domain, keep left
          go(acc ++ Chunk(sample1), c1.drop(1), c2.drop(1))
        }
        else if (ord.lt(sample1, sample2)) {
          // Left comes first, keep it
          go(acc ++ Chunk(sample1), c1.drop(1), c2)
        } else if (ord.gt(sample1, sample2)) {
          // Right comes first, keep it
          go(acc ++ Chunk(sample2), c1, c2.drop(1))
        }
        else (Chunk.empty, Chunk.empty, Chunk.empty) //invalid samples, domains not comparable
      } else {
        // If a chunk is empty, terminate so the caller will provide more samples
        (acc, c1, c2)
      }
    }

    if (c1.isEmpty && c2.isEmpty) (Chunk.empty, Chunk.empty, Chunk.empty)
    else if (c1.isEmpty) (c2, Chunk.empty, Chunk.empty)
    else if (c2.isEmpty) (c1, Chunk.empty, Chunk.empty)
    else go(Chunk.empty, c1, c2)
  }
}

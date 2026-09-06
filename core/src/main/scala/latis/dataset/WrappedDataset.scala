package latis.dataset

import cats.effect.IO
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.metadata.Metadata
import latis.model.*
import latis.ops.*
import latis.util.Identifier

/**
 * Wrap a Dataset with the ability to modify Operations
 * 
 * This manages the operations applied to it then optimizes
 * their application, possibly adding additional operations,
 * before applying them to the wrapped dataset. This can be
 * used to apply specific operations while front-loading those
 * (e.g. user time selections) that can be pushed down by the 
 * wrapped dataset. Note that any operations that the wrapped 
 * dataset already has (e.g. by a fdml processing instruction) 
 * cannot be modified.
 * 
 * Note: This would not be needed if we could copy AdaptedDataset, 
 * but we don't currently have access to the Adapter.
 */
case class WrappedDataset private (
  metadata: Metadata,
  dataset: Dataset,
  operations: List[UnaryOperation] = List.empty,
  mungeOperations: List[UnaryOperation] => List[UnaryOperation]
) extends Dataset {

  override def model: DataType = {
    mungeOperations(operations)
      .foldM(dataset.model)((mod, op) => op.applyToModel(mod))
      .fold(throw _, identity)
  }

  override def samples: Stream[IO, Sample] = {
    // Modify operations then add to wrapped dataset
    // to give it a change to push down
    val ops = mungeOperations(operations)
    dataset.withOperations(ops).samples
  }

  override def withOperation(op: UnaryOperation): Dataset =
    //TODO: validate op with model
    WrappedDataset(metadata, dataset, operations :+ op, mungeOperations)

  override def unsafeForce(): MemoizedDataset = ???
}

object WrappedDataset {
  def wrap(
    dataset: Dataset,
    mungeOperations: List[UnaryOperation] => List[UnaryOperation],
    id: Option[Identifier]
  ): WrappedDataset = {
    //TODO: pass additional metadata?
    val md = id.map(Metadata.apply)
      .getOrElse(dataset.metadata)
    WrappedDataset(md, dataset, List.empty, mungeOperations)
  }
}

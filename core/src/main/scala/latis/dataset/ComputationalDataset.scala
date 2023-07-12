package latis.dataset

import cats.effect.IO

import latis.data.*
import latis.metadata.*
import latis.model.*
import latis.ops.UnaryOperation
import latis.util.LatisException

/**
 * Defines a Dataset as a computational function.
 * The function type is described as a DataType
 * to facilitate composition with other Datasets.
 */
case class ComputationalDataset(
  metadata: Metadata,
  model: DataType,
  function: Data => Either[LatisException, Data]
) extends Dataset {
  //Note, this kind of function, unlike a SF, can be evaluated for any Data type
  // including a Function (e.g. spectrum).

  //TODO: SF apply is DomainData => Either[LatisException, RangeData], change to Data?
  def eval(data: Data): Either[LatisException, Data] =
    function(data)

  /**
   * Uses double arrow (=>) to represent computational function.
   */
  override def toString: String = model match {
    case Function(domain, range) => s"$id: $domain => $range"
    case _ => throw LatisException("ComputationalDataset model must be a Function")
  }

  //TODO: need SampledDataset to do things this can't
  def samples: fs2.Stream[IO, Sample] = ???
  def operations: List[UnaryOperation] = List.empty
  def withOperation(op: UnaryOperation): Dataset = ???
  def unsafeForce(): MemoizedDataset = ???
}

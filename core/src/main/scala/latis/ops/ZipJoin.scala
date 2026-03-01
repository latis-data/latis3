package latis.ops

import cats.effect.*
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.LatisException

/**
 * Horizontally combine samples from two datasets pairwise.
 *
 * This expects the second dataset to be a function of Index.
 * Note that this can be accomplished by not projecting the
 * domain variable.
 * Samples will be produced until one dataset runs out.
 * This assumes that there are no duplicate range variable ids.
 *
 * Note that ZipJoin does not extend Join because it does not
 * require domain types to be the same.
 * TODO: does Join really need same domain or just some subclasses?
 *   vs BinaryOperation? no new vars?
 */
case class ZipJoin() extends BinaryOperation {
  //TODO: both 0-arity
  //TODO: deal with same variable ids
  //TODO: fill to keep all?

  override def applyToModel(model1: DataType, model2: DataType): Either[LatisException, DataType] = {
    val (d1, r1) = model1 match {
      case Function(d, r) => (d, r)
      case _ => ???
    }
    val (d2, r2) = model2 match {
      case Function(d, r) => (d, r)
      case _ => ???
    }
    if (d2.isInstanceOf[Index]) {
      val range = Tuple.fromSeq(model1.rangeVariables ++ model2.rangeVariables)
      model1 match {
        case Function(domain, _) => range.flatMap(r => Function.from(domain, r))
        case _ => range
      }
    } else {
      val msg = "ZipJoin requires second dataset to be a function of Index"
      LatisException(msg).asLeft
    }
  }

  // Keep domain from first sample and concatenate range variables
  override def applyToData(
    model1: DataType,
    stream1: Stream[IO, Sample],
    model2: DataType,
    stream2: Stream[IO, Sample],
  ): Either[LatisException, Stream[IO, Sample]] = {
    stream1.zip(stream2).map {
      case (Sample(d, r1), Sample(_, r2)) => Sample(d, r1 ++ r2)
    }.asRight
  }

}

package latis.ops

import cats.data.NonEmptyList
import cats.syntax.all.*

import latis.util.Identifier
import latis.util.LatisException

/**
 * Defines an operation that groups a dataset by the given variables
 * to generate a new domain with the range being a `count` variable
 * representing the number of samples with that domain value.
 */
class CountBy(ids: NonEmptyList[Identifier]) extends GroupByVariable(ids.toList *) {

  override def aggregation: Aggregation = CountAggregation()
}

object CountBy {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, CountBy] = args match {
    case id :: ids =>
      NonEmptyList(id, ids).traverse { id =>
        Either.fromOption(
          Identifier.fromString(id),
          LatisException(s"Invalid identifier: $id")
        )
      }.map(new CountBy(_))
    case Nil => LatisException("CountBy requires at least one variable identifier").asLeft
  }
}

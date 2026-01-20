package latis.ops

import cats.effect.IO
import fs2.*

import latis.data.*
import latis.model.DataType
import latis.util.Identifier
import latis.util.LatisException

/**
 * reduce a function to a scalar or tuple, like agg but can't result in function
 */
case class Integration(id: Identifier) {
  //TODO: sum, newton-cotes, ...
  //  only last/inner dimension? for now
  //  curry then agg

  //override def aggregateFunction(model: DataType): Stream[IO, Sample] => IO[Data] = ???

  //reuse to remove domain var, also for eval... curry?
  //projection not suitable, no agg so needs placeholder
  //
  //diff aggs could have diff result types?
  //  but not for this and eval
  //  so can't use any agg here? we could support just the ones that make sense, subclasses of Integration trait
  //  require curry to get 1D nested func? find integrand in nested func domain,
  //  then no cartesian restriction here, just in curry, curryLast(n)?
  //  * could we just curry and sum? compose?
  //dropDimension? reduce?
  //override def applyToModel(model: DataType): Either[LatisException, DataType] = ???
}

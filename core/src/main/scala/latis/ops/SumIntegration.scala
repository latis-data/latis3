package latis.ops

import cats.effect.IO
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.Identifier

/**
 * Integrates by summing the range data
 */
case class SumIntegration(id: Identifier) extends Integration {
  //TODO: use Sum operation
  //TODO: value type promoted to long or double, preserve type? risky for sum

  /**
   * Sum the range values of the given samples.
   * 
   * The model is not needed for this case.
   */
  def integrate(model: DataType, samples: Stream[IO, Sample]): IO[Data] = {
    samples.map(_.range).reduce(sum).compile.toList.map(_.head).map {
      case r :: Nil => r
      case rs       => TupleData(rs)
    }
  }

  // Sum two RangeData objects, from Sum
  private def sum(r1: RangeData, r2: RangeData): RangeData = {
    // Note that types should align since these are from Samples of the same dataset
    r1.zip(r2).map {
      case (Integer(i1), Integer(i2)) => Data.LongValue(i1 + i2)
      case (Number(n1), Number(n2)) => Data.DoubleValue(n1 + n2)
      case _ => Data.DoubleValue(Double.NaN) //validation should prevent this
    }
  }
  
}

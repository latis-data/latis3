package latis.ops

import cats.syntax.all.*

import latis.util.LatisException

object BinAverageByWidth {

  def builder: OperationBuilder = (args: List[String]) => args match {
    case width :: Nil =>
      width.toDoubleOption.toRight(LatisException("Bin width must be numeric"))
        .flatMap(w => BinAverageByWidth(w))
    case _ => LatisException("BinAverageByWidth requires a single numeric argument").asLeft
  }

  def apply(width: Double): Either[LatisException, GroupByBinWidth] = {
    GroupByBinWidth(width, StatsAggregation())
  }
}

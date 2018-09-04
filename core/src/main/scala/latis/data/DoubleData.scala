package latis.data

trait DoubleData extends ScalarData[Double]

object DoubleData {
  def apply(value: Double): DoubleData = new ScalarData(value) with DoubleData
  def unapply(data: DoubleData): Option[Double] = Option(data.value)
}
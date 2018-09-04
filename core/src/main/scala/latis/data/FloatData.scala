package latis.data

trait FloatData extends ScalarData[Float]

object FloatData {
  def apply(value: Float): FloatData = new ScalarData(value) with FloatData
  def unapply(data: FloatData): Option[Float] = Option(data.value)
}
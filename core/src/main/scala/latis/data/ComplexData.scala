package latis.data

trait ComplexData extends ScalarData[(Double, Double)]

object ComplexData {
  def apply(value: (Double, Double)): ComplexData = new ScalarData(value) with ComplexData
  def unapply(data: ComplexData): Option[(Double, Double)] = Option(data.value)
}
package latis.data

trait Real extends ScalarData[BigDecimal]

object Real {
  def apply(value: BigDecimal): Real = new ScalarData(value) with Real
  def unapply(data: Real): Option[BigDecimal] = Option(data.value)
}
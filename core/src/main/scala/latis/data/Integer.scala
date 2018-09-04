package latis.data

trait Integer extends ScalarData[BigInt]

object Integer {
  def apply(value: BigInt): Integer = new ScalarData(value) with Integer
  def unapply(data: Integer): Option[BigInt] = Option(data.value)
}
package latis.data

trait LongData extends ScalarData[Long]

object LongData {
  def apply(value: Long): LongData = new ScalarData(value) with LongData
  def unapply(data: LongData): Option[Long] = Option(data.value)
}
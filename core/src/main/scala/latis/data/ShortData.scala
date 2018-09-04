package latis.data

trait ShortData extends ScalarData[Short]

object ShortData {
  def apply(value: Short): ShortData = new ScalarData(value) with ShortData
  def unapply(data: ShortData): Option[Short] = Option(data.value)
}
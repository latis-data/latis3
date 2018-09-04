package latis.data

trait IntData extends ScalarData[Int]

object IntData {
  def apply(value: Int): IntData = new ScalarData(value) with IntData
  def unapply(data: IntData): Option[Int] = Option(data.value)
}
package latis.data

trait CharData extends ScalarData[Char]

object CharData {
  def apply(value: Char): CharData = new ScalarData(value) with CharData
  def unapply(data: CharData): Option[Char] = Option(data.value)
}

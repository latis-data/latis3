package latis.data

trait Text extends ScalarData[String]

object Text {
  def apply(value: Any): Text = new ScalarData(value.toString) with Text
  def unapply(data: Text): Option[String] = Option(data.value)
}

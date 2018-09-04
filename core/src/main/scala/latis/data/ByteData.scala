package latis.data

trait ByteData extends ScalarData[Byte]

object ByteData {
  def apply(value: Byte): ByteData = new ScalarData(value) with ByteData
  def unapply(data: ByteData): Option[Byte] = Option(data.value)
}
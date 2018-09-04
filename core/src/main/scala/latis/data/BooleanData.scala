package latis.data

trait BooleanData extends ScalarData[Boolean]

object BooleanData {
  
  val True  = BooleanData(true)
  val False = BooleanData(false)
  
  def apply(value: Boolean): BooleanData = new ScalarData(value) with BooleanData
  def unapply(data: BooleanData): Option[Boolean] = Option(data.value)
}
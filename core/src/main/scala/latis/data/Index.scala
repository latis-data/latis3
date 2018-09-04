package latis.data

/*
 * TODO: special treatment for Index
 * needed as part of types, adapter needs to know not to look for value
 * auto increment?
 */

trait Index extends ScalarData[Int]

object Index {
  def apply(value: Int): Index = new ScalarData(value) with Index
  def unapply(data: Index): Option[Int] = Option(data.value)
}

package latis.util

/**
 * A class that defines "properties" as a Seq of String pairs
 * can extend this trait to get a set of methods to access
 * the properties like they would a LatisConfig.
 */
trait ConfigLike {

  def properties: Seq[(String, String)]

  val propertyMap: Map[String, String] = properties.toMap

  def get(key: String): Option[String] = getString(key)

  def getString(key: String): Option[String] =
    propertyMap.get(key)

  def getOrElse(key: String, default: String): String =
    propertyMap.getOrElse(key, default)

  def getInt(key: String): Option[Int] =
    propertyMap.get(key).map(_.toInt)

  def getOrElse(key: String, default: Int): Int =
    getInt(key).getOrElse(default)
}

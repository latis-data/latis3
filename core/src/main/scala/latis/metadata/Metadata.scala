package latis.metadata

import latis.util.PropertiesLike

/**
 * Class to represent metadata as name-value pairs.
 */
class Metadata(val properties: Map[String, String]) extends PropertiesLike {
  
  def id: String = getProperty("id", "")
}

object Metadata {
  def apply(id: String): Metadata = Metadata("id" -> id)
  def apply(props: (String, String)*): Metadata = new Metadata(props.toMap)
  def apply(props: Map[String, String]): Metadata = new Metadata(props)
}
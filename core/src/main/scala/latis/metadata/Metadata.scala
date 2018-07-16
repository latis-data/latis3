package latis.metadata

import latis.util.PropertiesLike

/**
 * Class to represent metadata as name-value pairs.
 */
case class Metadata(properties: Map[String, String]) extends PropertiesLike

object Metadata {
  
  def apply(props: (String, String)*): Metadata = Metadata(props.toMap)
}
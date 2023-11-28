package latis.metadata

import latis.util.Identifier
import latis.util.PropertiesLike

/**
 * Class to represent metadata as name-value pairs.
 */
class Metadata(val properties: Map[String, String]) extends PropertiesLike with Serializable {

  /** Add or replace the given property */
  def +(kv: (String, String)): Metadata = Metadata(properties + kv)

  /** Remove the given property */
  def -(key: String): Metadata = Metadata(properties - key)

  /** Combine the properties */
  def ++(md: Metadata): Metadata = Metadata(properties ++ md.properties)
}

object Metadata {

  /**
   * Construct Metadata with the given identifier.
   */
  def apply(id: Identifier): Metadata = Metadata("id" -> id.asString)

  /**
   * Construct Metadata from a comma separated list of name-value pairs.
   * Each pair can also be given as "foo" -> "bar".
   */
  def apply(props: (String, String)*): Metadata = new Metadata(props.toMap)

  /**
   * Construct Metadata from a Map of name-value properties.
   */
  def apply(props: Map[String, String]): Metadata = new Metadata(props)
}

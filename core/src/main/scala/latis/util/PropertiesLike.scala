package latis.util

/**
 * Extend this trait to get a more convenient API for accessing properties
 * for any class that provides a method (or val) that returns a Map of 
 * String properties.
 */
trait PropertiesLike {
  
  /**
   * Abstract method to return a Map of properties.
   * This is used to support the other functionality.
   */
  def properties: Map[String, String]

  /**
   * Return Some property value or None if the property does not exist.
   */
  def getProperty(name: String): Option[String] = properties.get(name)
  
  /**
   * Return property value or default if property does not exist.
   */
  def getProperty(name: String, default: String): String = 
    getProperty(name).getOrElse(default)
  
  /**
   * Provide an explicit unsafe get method to directly access a
   * property value without Option.
   * May throw Exception.
   */
  def unsafeGet(name: String): String = properties(name)
  
}

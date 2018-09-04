package latis.metadata

/**
 * Trait to provide direct Metadata access.
 * This trait can be mixed in with any class that provides "metadata".
 * Users of that class can then access metadata values via the "apply" method.
 */
trait MetadataLike {
  
  /**
   * Abstract method for subtypes to provide metadata.
   */
  def metadata: Metadata
  
  /**
   * Provide optional metadata values by name.
   */
  def apply(name: String): Option[String] = metadata.getProperty(name)

}

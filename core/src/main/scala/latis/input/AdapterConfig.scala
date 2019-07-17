package latis.input

import latis.util.ConfigLike

/**
 * Trait for an Adapter configuration.
 * Instances of this trait will be used by the AdapterFactory
 * to construct the specified instance of an Adapter as defined
 * by the required className. These instances may also contain
 * definitions for configurable properties of the Adapter.
 */
case class AdapterConfig(properties: (String, String)*) extends ConfigLike {

  /**
   * An AdapterConfig must define a clasName.
   * This will fail upon construction if this
   * property is not defined.
   */
  val className: String = get("className").getOrElse {
    val msg = "An adapter must have a className defined."
    throw new RuntimeException(msg)
  }
}

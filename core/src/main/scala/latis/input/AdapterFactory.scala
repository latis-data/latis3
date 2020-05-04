package latis.input

import latis.model.DataType
import latis.util.ReflectionUtils.callMethodOnCompanionObject

/**
 * The companion object of an Adapter class can extend AdapterFactory
 * to enable it to be constructed via AdapterFactory.makeAdapter.
 */
trait AdapterFactory {
  def apply(model: DataType, config: AdapterConfig): Adapter
}

/**
 * Dynamically construct an Adapter for the given DataType and AdapterConfig.
 * This assumes that the Adapter specified by the className in the config has
 * a companion object that extends the AdapterFactory trait.
 */
object AdapterFactory {

  def makeAdapter(model: DataType, config: AdapterConfig, cl: ClassLoader): Adapter =
    try {
      callMethodOnCompanionObject(cl, config.className, "apply", model, config).asInstanceOf[Adapter]
    } catch {
      case e: Exception =>
        throw new RuntimeException("Failed to construct Adapter: " + config.className, e)
    }

  def makeAdapter(model: DataType, config: AdapterConfig): Adapter =
    makeAdapter(model, config, getClass().getClassLoader())

}

package latis.input

import latis.model.DataType
import latis.util.ReflectionUtils.callMethodOnCompanionObject

/**
 * Dynamically construct an Adapter for the given DataType and AdapterConfig.
 * This assumes that the Adapter specified by the className in the config has
 * the comparable "apply" method in its companion object.
 */
object AdapterFactory {
  
  def makeAdapter(model: DataType, config: AdapterConfig): Adapter = {
    try {
      callMethodOnCompanionObject(config.className, "apply", model, config).asInstanceOf[Adapter]
    } catch {
      case e: Exception =>
        throw new RuntimeException("Failed to construct Adapter: " + config.className, e)
    }
  }

}
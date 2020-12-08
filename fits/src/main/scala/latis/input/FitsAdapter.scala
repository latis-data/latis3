package latis.input

import java.net.URI

import latis.data.Data
import latis.model.DataType
import latis.ops.Operation
import latis.util.ConfigLike

case class FitsAdapter(
  model: DataType,
  config: FitsAdapter.Config = FitsAdapter.Config()
) extends Adapter {

  def getData(baseUri: URI, ops: Seq[Operation]): Data = ???

}

object FitsAdapter extends AdapterFactory {
  def apply(model: DataType, config: AdapterConfig): Adapter =
    new FitsAdapter(model, FitsAdapter.Config(config.properties: _*))

  /**
   * Defines a FitsAdapter specific configuration.
   */
  case class Config(properties: (String, String)*) extends ConfigLike
}

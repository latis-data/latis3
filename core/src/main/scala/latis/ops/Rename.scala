package latis.ops

import latis.data.Data
import latis.model._

/**
 * Define an Operation that renames a specific variable within a Dataset.
 * This only impacts the model.
 */
case class Rename(origName: String, newName: String) extends UnaryOperation {

  def applyToModel(model: DataType): DataType =
    model.map {
      case v if (v.id == origName) => v.rename(newName)
      //TODO: support aliases with hasName
      case v => v
    }

  /**
   * Provides a no-op implementation for Rename.
   */
  def applyToData(data: Data, model: DataType): Data = data
}

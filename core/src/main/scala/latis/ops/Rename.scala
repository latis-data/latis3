package latis.ops

import latis.model._

/**
 * Define an Operation that renames a specific variable within a Dataset.
 * This only impacts the model.
 */
class Rename(origName: String, newName: String) extends UnaryOperation {

  override def applyToModel(model: DataType): DataType =
    model.map {
      case v if (v.id == origName) => v.rename(newName)
      //TODO: support aliases with hasName
      case v => v
    }
}

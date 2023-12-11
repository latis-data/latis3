package latis.dataset

import latis.metadata.Metadata
import latis.model.DataType
import latis.ops.UnaryOperation

/**
 * Defines the base capabilities for a Dataset.
 */
abstract class AbstractDataset(
  _metadata: Metadata,
  _model: DataType,
  val operations: List[UnaryOperation]
) extends Dataset with Serializable {

  /**
   * Adds provenance to the metadata of the resulting Dataset
   * as derived from the operations applied.
   */
  val metadata: Metadata = {
    // Gather the provenance from each operation
    //TODO: add time label, version, ...
    //TODO: provide new name/id?
    val provenance: String =
      operations
        .map(_.provenance)
        .mkString(System.lineSeparator)

    // Update the provenance metadata
    val history = _metadata.getProperty("history") match {
      case Some(h) => h + System.lineSeparator + provenance
      case None    => provenance
    }
    _metadata + ("history" -> history)
  }

  /**
   * Applies the sequence of Operations to the
   * original model returning the new model.
   */
  val model: DataType =
    operations.foldLeft(_model)((mod, op) => op.applyToModel(mod).fold(throw _, identity))

}

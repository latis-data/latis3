package latis.ops

import latis.model._
import latis.metadata._
import latis.data._


/**
 * An Operation that acts on a single Dataset.
 */
trait UnaryOperation extends Operation {
  
  /**
   * Provide a new model resulting from this Operations.
   * Default to no-op.
   */
  def applyToModel(model: DataType): DataType = model
  
  /**
   * Provide new Data resulting from this Operation.
   * Note that we get the Dataset here and not just the input Data
   * since data operations often need the metadata/model.
   * Default to no-op.
   */
  def applyToData(data: SampledFunction, model: DataType): SampledFunction = data
  //TODO: awkward that this also takes a model: pass Dataset for all?
  
  /**
   * Add provenance to the Dataset's metadata.
   * //TODO: final? 
   *   what else might need to change?
   *   contact info...
   *   override needs to call super
   *   add prov in diff step?
   */
  def applyToMetadata(md: Metadata): Metadata = {
    val history = md.getProperty("history") match {
      case Some(h) => h + "\n" + provenance  //TODO: StringUtils.newline
      case None => provenance
    }
    md + ("history" -> history)
  }
  
  /**
   * Apply this Operation to the given Dataset and provide a new Dataset
   * with updated Metadata and Data.
   */
  def apply(ds: Dataset): Dataset = ds match {
    case Dataset(md, model, data) => 
      Dataset(
        applyToMetadata(md), 
        applyToModel(model), 
        applyToData(data, model)
      )
  }

}

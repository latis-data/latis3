package latis.ops

import java.net.URI

import latis.data._
import latis.input._
import latis.model.DataType

/**
 * Given a "granule list" Dataset and an Adapter to parse each granule,
 * combine the data from each granule into a single Dataset.
 * The dataset must have a "uri" variable that is not in a nested Function.
 * This assumes that each granule has the same model and can be appended
 * to the previous granule.
 */
case class GranuleListJoin(model: DataType, adapter: Adapter) extends UnaryOperation {
  
  /**
   * Replace the original model (of the granule list dataset) 
   * with the model of the granules. 
   */
  override def applyToModel(_model: DataType): DataType = model
  
  /**
   * Apply the Adapter to each URI in the granule list dataset
   * to generate a SampledFunction for each and wrap them all
   * in a CompositeSampledFunction.
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    
    // Get the position of the "uri" value within a Sample
    val pos: SamplePosition = model.getPath("uri") match {
      case Some(path) if (path.length == 1) => path.head
      case _ => ??? //TODO: error, no non-nested uri
    }
    
    // Make function that can be mapped over the granule list data.
    // Extract the uri then apply that to the Adapter to get the data for that granule.
    val f: Sample => SampledFunction = (sample: Sample) => {
      sample.getValue(pos) match {
        case Some(Text(s)) =>
          val uri = new URI(s) //TODO: error
          adapter.getData(uri) //TODO: delegate ops?
        case _ => ??? //TODO: error if type is wrong, position should be valid
      }
    }
    
    // Create SampledFunctions for each granule URI 
    // and combine into a CompositeSampledFunction.
    // Note the unsafeForce so we can get the Seq out of IO.
    CompositeSampledFunction(data.unsafeForce.samples.map(f))
  }
  
}


object GranuleListJoin {

  def apply(model: DataType, config: AdapterConfig): GranuleListJoin =
    GranuleListJoin(model, AdapterFactory.makeAdapter(model, config))
}




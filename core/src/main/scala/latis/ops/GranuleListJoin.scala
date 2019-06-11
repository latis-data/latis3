package latis.ops

import latis.data._
import latis.model._
import latis.input.Adapter
import java.net.URI

/**
 * Given a "granule list" Dataset and an Adapter to parse each granule,
 * combine the data from each granule into a single Dataset.
 * The dataset must have a "uri" variable that is not in a nested Function.
 */
case class GranuleListJoin(adapter: Adapter) extends UnaryOperation {
  
  /*
   * TODO: model needs to become model of first dataset
   *   same for metedata, for now
   * A full join operation could preserve that
   *   assuming append join here via flatMap
   *   generalize to any join? 
   * adapter probably already needs the model
   * how would we construct/config a GLJ dataset?
   * 
   */
  
  
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    
    // Get the position of the "uri" value within a Sample
    val pos: SamplePosition = model.getPath("uri") match {
      case Some(path) if (path.length == 1) => path.head
      case _ => ??? //TODO: error, no non-nested uri
    }
    
    // Make function that can be mapped over the ganules list data.
    // Extract the uri then apply that to the Adapter to get the data for that granule.
    val f: Sample => MemoizedFunction = (sample: Sample) => {
      sample.getValue(pos) match {
        case Some(s: String) =>
          val uri = new URI(s) //TODO: error
          adapter(uri).unsafeForce //Note, we need a MemoizedFunction for flatMap
      }
    }
      
    data.flatMap(f)
  }
  
}

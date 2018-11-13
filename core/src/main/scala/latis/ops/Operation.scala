package latis.ops

import latis.model._
import latis.metadata._
import latis.data._


/**
 * An Operation implements a component of the functional algebra 
 * that operates on Datasets.
 */
trait Operation {
  /*
   * TODO: CompositeOperation(ops)
   * "compile" ops
   * compose data functions
   * can't forget model and prov
   */
  
  /**
   * Provide an indication that this operation was applied 
   * for the provenance (history) metadata.
   */
  def provenance: String = this.toString

  
  //def apply(ds: Dataset*): Dataset

}

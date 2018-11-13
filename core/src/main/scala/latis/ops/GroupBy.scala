package latis.ops

import latis.model._
import latis.metadata._
import latis.data._

/**
 * Operation to restructure a Dataset by defining a new domain
 * made up of the given variables.
 */
case class GroupBy(vnames: String*) extends UnaryOperation {
  
  /*
   * TODO: Generalize model application
   * preserve nested Tuples?
   * 
   * Assume hysics use case for now:
   *   (iy, ix, w) -> f  =>  (ix, iy) -> w -> f
   * Note that this also handles the transpose.
   */
  override def applyToModel(model: DataType): DataType = model match {
    case Function(Tuple(iy, ix, w), f) => Function(Tuple(ix, iy), Function(w, f))
    case _ => ???
  }
  
  /**
   * Convert the variable information from the model into SamplePaths
   * that can be applied to the model-agnostic SampledFunction.
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    val paths = vnames.flatMap(model.getPath(_))  //TODO: deal with invalid vname
    data.groupBy(paths: _*)
  }
  
}

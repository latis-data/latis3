package latis.ops

import latis.model._
import latis.metadata._
import latis.data._

/**
 * Operation to keep only Samples with a variable 
 * that matches one of the given values.
 */
case class Contains(vname: String, values: Any*) extends Filter {
  //TODO: support nested functions, aliases,... (See Selection)
  
  def makePredicate(model: DataType): Sample => Boolean = {
    // Determine the path to the selected variable
    val path = model.getPath(vname) match {
      case Some(p) => p.head  //assume no nested functions for now
      case None => ??? //TODO: invalid path
    }
        
    // Convert values to appropriate type for comparison
    val cvals = model.find(_.id == vname) match {
      case Some(s: Scalar) => values.map(v => s.parseValue(v.toString))
      case _ => ??? //error
    }
    
    // Define predicate function
    (sample: Sample) => sample.getValue(path) match {
      case Some(v) => cvals.contains(v) //TODO: would "contains" be more efficient with a HashSet...?
      case None => ??? //TODO: invalid sample position, catch earlier - not for each sample, presumably derived from model so shouldn't happen
    }
  }
  
}

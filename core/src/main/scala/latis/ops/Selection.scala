package latis.ops

import latis.data._
import latis.metadata._
import latis.model._

/**
 * Operation to keep only Samples that meet the selection criterion.
 */
case class Selection(vname: String, operator: String, value: String) extends Filter {
  //TODO: use smaller types, enumerate operators?
  //TODO: support aliases
  //TODO: error if var not found?
  //TODO: enable IndexedFunction to use binary search...
  //TODO: support nested functions, all or none?
  //TODO: pass any value type? toString then parse?

  
  def makePredicate(model: DataType): Sample => Boolean = {
    // Determine the path to the selected variable
    val path = model.getPath(vname) match {
      case Some(p) => p.head  //assume no nested functions for now
      case None => ??? //TODO: invalid path
    }
    
    // Convert selection value to appropriate type for comparison
    val cval = model.find(_.id == vname) match {
      case Some(s: Scalar) => s.parseValue(value)
      case _ => ??? //error
    }
    
    // Define predicate function
    (sample: Sample) => sample.getValue(path) match {
      case Some(v) => matches(ScalarOrdering.compare(v, cval))
      case None => ??? //TODO: invalid sample position, catch earlier - not for each sample, presumably derived from model so shouldn't happen
    }
  }

  /**
   * Helper function to determine if the value comparison
   * satisfies the selection operation.
   */
  private def matches(comparison: Int): Boolean = {
    if (operator == "!=") {
      comparison != 0
    } else {
      (comparison < 0 && operator.contains("<")) ||
      (comparison > 0 && operator.contains(">")) ||
      (comparison == 0 && operator.contains("="))
    }
  }
}


object Selection {
  
  def apply(expression: String): Selection = {
    //TODO: beef up expression parsing
    val ss = expression.split("\\s+") //split on whitespace
    Selection(ss(0), ss(1), ss(2))
  }
}
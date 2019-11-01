package latis.ops

import latis.data.Data
import latis.data.Sample
import latis.data.SampleOps
import latis.data.SamplePosition
import latis.model.DataType
import latis.model.Scalar

/**
 * Operation to keep only Samples that meet the given selection criterion.
 */
case class Selection(vname: String, operator: String, value: String)
    extends Filter {
  //TODO: use smaller types, enumerate operators?
  //TODO: enable IndexedFunction to use binary search...
  //TODO: support nested functions, all or none?
  //TODO: allow value to have units

  def makePredicate(model: DataType): Sample => Boolean = {
    // Get the desired Scalar from the model
    //TODO: support aliases
    val scalar: Scalar = model.find(_.id == vname) match {
      case Some(s: Scalar) => s
      case _ =>
        val msg = s"Selection variable not found: $vname"
        throw new UnsupportedOperationException(msg)
    }

    // Determine the Sample position of the selected variable
    val pos: SamplePosition = model.getPath(vname) match {
      case Some(p) => p.length match {
        case 1 => p.head
        case _ =>
          val msg = "Selection does not support values in nested Functions."
          throw new UnsupportedOperationException(msg)
      }
      case None => ??? //shouldn't happen due to earlier check
    }

    // Convert selection value to appropriate type for comparison
    val cdata: Data = scalar.convertValue(value)

    // Get the Ordering from the Scalar
    val ordering: Ordering[Data] = scalar.ordering

    // Define predicate function
    (sample: Sample) => sample.getValue(pos) match {
      case Some(d) => matches(ordering.compare(d, cdata))
      case None => ??? //shouldn't happen since the pos came from the model
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

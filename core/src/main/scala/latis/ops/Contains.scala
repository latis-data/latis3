package latis.ops

import latis.data._
import latis.model._
import latis.util.Identifier
import latis.util.LatisException

/**
 * Defines an Operation to keep only Samples with a variable
 * that matches one of the given values.
 */
case class Contains(id: Identifier, values: String*) extends Filter {
  //TODO: support nested functions, aliases,... (See Selection)

  def predicate(model: DataType): Sample => Boolean = {
    // Determine the path to the selected variable
    val path: SamplePosition = model.findPath(id) match {
      case Some(p) => p.head //assume no nested functions for now
      case None => ???    //TODO: invalid path or vname
    }

    // Convert values to appropriate type for comparison
    val cvals: Seq[Datum] = model.findVariable(id) match {
      case Some(s: Scalar) => values.map { v =>
        s.convertValue(v).getOrElse {
          val msg = s"Invalid comparison value: $v"
          throw LatisException(msg)
        }
      }
      case _  => ??? //bug, invalid path
    }

    // Define predicate function
    (sample: Sample) =>
      sample.getValue(path).map { d =>
        cvals.contains(d)  // Uses "==" or "equals", should work for Datum value classes
      }.getOrElse {
        ??? //bug, we should have a valid path at this point
      }
  }

}

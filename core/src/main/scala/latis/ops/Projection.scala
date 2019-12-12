package latis.ops

import latis.data._
import latis.model._

/**
 * Operation to project only a given set of variables in a Dataset.
 */
case class Projection(vids: String*) extends MapOperation {
  //TODO: support nested Functions
  //TODO: support aliases, hasName
  //TODO: support dot notation for nested tuples
  //TODO: Index place holders

  override def applyToModel(model: DataType): DataType =
    applyToVariable(model).getOrElse {
      throw new RuntimeException("Nothing projected")
    }

  /**
   * Recursive method to apply the projection.
   */
  private def applyToVariable(v: DataType): Option[DataType] = v match {
    case s: Scalar =>
      if (vids.contains(s.id)) Some(s) else None
    case Tuple(vars @ _*) =>
      val vs = vars.flatMap(applyToVariable)
      vs.length match {
        case 0 => None // drop empty Tuple
        case 1 => Some(vs.head) // reduce Tuple of one
        case _ => Some(Tuple(vs))
      }
    case Function(d, r) =>
      (applyToVariable(d), applyToVariable(r)) match {
        case (Some(d), Some(r)) => Some(Function(d, r))
        case _ =>
          throw new UnsupportedOperationException(
            "Both domain and range portions must be projected."
          )
      }
  }

  override def mapFunction(model: DataType): Sample => Sample = {
    // Compute sample positions once to minimize Sample processing
    //TODO: error if vid not found, we just drop them here
    val samplePositions = vids.flatMap(model.getPath).map(_.head)

    // Get the indices of the projected variables in the Sample.
    // Sort since the FDM requires original order of variables.
    // TODO: could we allow range to be reordered?
    import scala.language.postfixOps
    //val domainIndices: Seq[Int] = samplePositions.collect { case DomainPosition(i) => i } sorted
    val rangeIndices: Seq[Int]  = samplePositions.collect { case RangePosition(i)  => i } sorted

    (sample: Sample) => sample match {
      case Sample(ds, rs) =>
        //val domain = domainIndices.map(ds(_)) //TODO: fill non-projected domain with Index
        val range = rangeIndices.map(rs(_))
        Sample(ds, range)
    }
  }

}

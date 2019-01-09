package latis.ops

import latis.data._
import latis.model.DataType

/**
 * Operation to project only a given set of variables in a Dataset.
 */
case class Projection(vids: String*) extends MapOperation {
  //TODO: support nested Functions
  //TODO: support aliases, hasName
  //TODO: support dot notation for nested tuples
  
  override def applyToModel(model: DataType): DataType = {
    //TODO: consider how to handle Tuples of 0, 1; preserve namespace
    model.filter(dt => vids.contains(dt.id))
  }
  
  override def makeMapFunction(model: DataType): Sample => Sample = {
    // Compute sample positions once to minimize Sample processing
    //TODO: error if vid not found, we just drop them here
    val samplePositions = vids.flatMap(model.getPath(_)).map(_.head)
    
    // Get the indices of the projected variables in the Sample.
    // Sort since the FDM requires original order of variables.
    // TODO: could we allow range to be reordered?
    import scala.language.postfixOps
    val domainIndices: Seq[Int] = samplePositions collect { case DomainPosition(i) => i } sorted
    val rangeIndices:  Seq[Int] = samplePositions collect { case RangePosition(i)  => i } sorted
    
    (sample: Sample) => sample match {
      case Sample(ds, rs) =>
        //val domain = domainIndices.map(ds(_)) //TODO: fill non-projected domain with Index
        val range  = rangeIndices.map(rs(_))
        Sample(ds, range)
    }
  }
  
}
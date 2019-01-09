package latis.data

/**
 * Encode the position of a scalar value or nested SampledFunction within a Sample.
 * This is used to build functions that can operate on Samples without knowing the model.
 * Note that there is no need to represent Tuples here since Samples do not preserve them.
 * Positions in the range can point to a nested SampledFunction so we need a
 * SamplePath (Seq[SamplePosition]) to locate a variable in the model.
 */
trait SamplePosition

//TODO: enforce no negative values
case class DomainPosition(i: Int) extends SamplePosition

case class RangePosition(i: Int) extends SamplePosition

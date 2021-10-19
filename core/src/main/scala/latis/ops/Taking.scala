package latis.ops

/**
 * A UnaryOperation implementing the Taking trait preserves
 * a subset of Samples without needing to examine the data or
 * otherwise altering the data. A Taking operation is idempotent
 * (i.e. it can be applied more than once with no effect
 * beyond the first application).
 *
 * This property enables distribution over joins with reapplication
 * after the join.
 *
 * Examples: Head, Take, TakeRight, Last
 *
 * Counter-examples (not Taking):
 *  - Selection requires data examination
 *  - Tail is not idempotent
 */
trait Taking

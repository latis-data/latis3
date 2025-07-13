package latis.util

/**
 * [[Bounds]] represents the lower and upper bounds of a range.
 *
 * It enforces that the upper bound is strictly greater than
 * the lower bound.
 */
final case class Bounds[T] private (lower: T, upper: T)(ord: Ordering[T]) {

  /**
   * Tests whether the given value falls within this [[Bounds]].
   *
   * The lower bound is always inclusive but the upper bound defaults
   * to being exclusive. NaN is never within bounds.
   *
   * @param value the value being tested
   * @param inclusive specifies if the upper bound is inclusive,
   *                  default is false
   */
  def contains(value: T, inclusive: Boolean = false): Boolean = {
    //Note that the default Double.TotalOrdering (unlike IeeeOrdering) will treat
    // NaN greater than other values but the less than test will be false.
    ord.gteq(value, lower) && (ord.lt(value, upper) || (inclusive && ord.equiv(value, upper)))
  }

  /**
   * Constrains the lower bound.
   *
   * This optionally returns a new Bounds if the given value is greater than
   * the current lower bound. Otherwise, it returns itself. If the bounds
   * become invalid: lower > upper, this will return None.
   *
   * @param value the potentially new lower bound
   */
  def constrainLower(value: T): Option[Bounds[T]] = {
    if (ord.gt(value, lower)) Bounds.of(value, upper)(ord)
    else Some(this)
  }

  /**
   * Constrains the upper bound.
   *
   * This optionally returns a new Bounds if the given value is less than the
   * current upper bound. Otherwise, it returns itself. If the bounds become
   * invalid: lower > upper, this will return None.
   *
   * @param value the potentially new upper bound
   */
  def constrainUpper(value: T): Option[Bounds[T]] = {
    if (ord.lt(value, upper)) Bounds.of(lower, value)(ord)
    else Some(this)
  }
}

object Bounds {

  /** Tries to construct a [[Bounds]] from lower and upper values. */
  def of[T](lower: T, upper: T)(implicit ord: Ordering[T]): Option[Bounds[T]] =
    (lower, upper) match {
      //Need special handling for NaN since default TotalOrdering.lt(_, NaN) is true
      case (_, u: Double) if (u.isNaN) => None
      case (_, u: Float)  if (u.isNaN) => None
      case (l, u) if (ord.lt(l, u)) => Some(new Bounds[T](lower, upper)(ord))
      case _ => None
    }

}

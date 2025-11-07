package latis.util

/**
 * [[Bounds]] represent the lower and upper extents of a coverage.
 *
 * Note that "lower" and "upper" bounds are compared based on an
 * Ordering, not necessarily the usual numeric ordering.
 */
sealed trait Bounds[T] {

  /** Is the given value covered by this [[Bounds]] */
  def contains(value: T): Boolean

  /** Does this [[Bounds]] have a zero coverage */
  def isEmpty: Boolean

  /**
   * Replaces the lower bound if the given value further constrains it.
   * If the upper bound does not cover the value, this will return an
   * empty [[Bounds]].
   */
  def constrainLower(value: T): Bounds[T]

  /**
   * Replaces the upper bound if the given value further constrains it.
   * If the lower bound does not cover the value, this will return an
   * empty [[Bounds]].
   */
  def constrainUpper(value: T): Bounds[T]
}

/**
 * [[NonEmptyBounds]] represent a coverage with an upper [[Bound]]
 * that is greater than the lower [[Bound]].
 */
case class NonEmptyBounds[T] private[util] (lower: Bound[T], upper: Bound[T])
  (using ord: Ordering[T]) extends Bounds[T] {

  def contains(value: T): Boolean =
    lower.boundsLower(value) && upper.boundsUpper(value)

  def isEmpty: Boolean = false

  def constrainLower(value: T): Bounds[T] = {
    if (lower.boundsLower(value)) {
      if (upper.boundsUpper(value))
        NonEmptyBounds(lower.updated(value), upper)
      else Bounds.empty
    }
    else this
  }

  def constrainUpper(value: T): Bounds[T] = {
    if (upper.boundsUpper(value)) {
      if (lower.boundsLower(value))
        NonEmptyBounds(lower, upper.updated(value))
      else Bounds.empty
    }
    else this
  }

  override def toString: String = (lower, upper) match {
    case (ClosedBound(l), ClosedBound(u)) => s"[$l,$u]"
    case (ClosedBound(l), OpenBound(u))   => s"[$l,$u)"
    case (OpenBound(l), ClosedBound(u))   => s"($l,$u]"
    case (OpenBound(l), OpenBound(u))     => s"($l,$u)"
  }
}

/** [[Bounds]] that contain a single value */
case class PointBounds[T](point: T) extends Bounds[T] {
  def contains(value: T): Boolean = point == value
  def isEmpty: Boolean = false
  def constrainLower(value: T): Bounds[T] =
    if (point == value) this else Bounds.empty
  def constrainUpper(value: T): Bounds[T] =
    if (point == value) this else Bounds.empty
  override def toString: String = s"[$point]"
}

/** [[Bounds]] that cover nothing */
class EmptyBounds[T] extends Bounds[T] {
  def contains(value: T): Boolean = false
  def isEmpty: Boolean = true
  def constrainLower(value: T): Bounds[T] = Bounds.empty
  def constrainUpper(value: T): Bounds[T] = Bounds.empty
  override def toString: String = "0"
}

object Bounds {

  def empty[T]: Bounds[T] = new EmptyBounds

  /**
   * Constructs [[Bounds]] with an inclusive lower bound and
   * exclusive upper bound.
   *
   * If either bound is a NaN, this will return an empty [[Bounds]].
   */
  def of[T](lower: T, upper: T)(using ord: Ordering[T]): Bounds[T] =
    //Need special handling for NaN
    (lower, upper) match {
      case (l: Double, u: Double) if (l.isNaN || u.isNaN) => Bounds.empty
      case (l: Float, u: Float)   if (l.isNaN || u.isNaN) => Bounds.empty
      case (l, u) if (ord.lt(lower, upper)) =>
        NonEmptyBounds(ClosedBound(lower), OpenBound(upper))
      case _ => Bounds.empty
    }

  /**
   * Constructs [[Bounds]] with inclusive lower and upper bounds.
   */
  def inclusive[T](lower: T, upper: T)(using ord: Ordering[T]): Bounds[T] = {
    if (ord.lt(lower, upper))
      NonEmptyBounds(ClosedBound(lower), ClosedBound(upper))
    else if (lower == upper) PointBounds(lower)
    else Bounds.empty
  }
}


/**
 * [[Bound]] defines a boundary (lower or upper) of a coverage.
 */
sealed trait Bound[T] {
  /** Returns the bounding value */
  def value: T
  /** Does this bound the given value on the lower side */
  def boundsLower(value: T): Boolean
  /** Does this bound the given value on the upper side */
  def boundsUpper(value: T): Boolean
  /** Returns a new Bound with the updated value */
  def updated(value: T): Bound[T]
}

/** Defines a [[Bound]] that is inclusive. */
final case class ClosedBound[T](val value: T)
(using ord: Ordering[T]) extends Bound[T] {
  def boundsLower(v: T): Boolean = ord.lteq(value, v)
  def boundsUpper(v: T): Boolean = ord.gteq(value, v)
  def updated(v: T): Bound[T] = ClosedBound[T](v)
}

/** Defines a [[Bound]] that is exclusive. */
final case class OpenBound[T](val value: T)
(using ord: Ordering[T]) extends Bound[T] {
  def boundsLower(v: T): Boolean = ord.lt(value, v)
  def boundsUpper(v: T): Boolean = ord.gt(value, v)
  def updated(v: T): Bound[T] = OpenBound[T](v)
}

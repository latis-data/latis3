package latis.util

import cats.*

/**
 * [[Bounds]] represent the lower and upper extents of a coverage.
 *
 * Note that "lower" and "upper" bounds are compared based on a
 * PartialOrder, not necessarily the usual lexical ordering.
 */
sealed trait Bounds[T] {

  /** Is the given value covered by this [[Bounds]] */
  def contains(value: T)(using ord: PartialOrder[T]): Boolean = this match {
    case NonEmptyBounds(l, u) => l.boundsLower(value) && u.boundsUpper(value)
    case PointBounds(v)       => ord.eqv(v, value)
    case _: EmptyBounds[T]    => false
  }

  /** Does this [[Bounds]] have no coverage */
  def isEmpty: Boolean = this match {
    case _: EmptyBounds[t] => true
    case _ => false
  }

  /** Is this [[Bounds]] without an infinite extent */
  def isFinite: Boolean = this match {
    case NonEmptyBounds(_: InfiniteBound[T], _) => false
    case NonEmptyBounds(_, _: InfiniteBound[T]) => false
    case _ => true
  }

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
 * that is "greater than" the lower [[Bound]] based on the ordering.
 */
final case class NonEmptyBounds[T: PartialOrder] private[util] (
  lower: Bound[T], upper: Bound[T]
) extends Bounds[T] {

  def constrainLower(value: T): Bounds[T] = {
    if (lower.boundsLower(value)) {
      if (upper.boundsUpper(value))
        lower match {
          case _: ClosedBound[T]   => NonEmptyBounds(ClosedBound(value), upper)
          case _: OpenBound[T]     => NonEmptyBounds(OpenBound(value), upper)
          // Note, a lower infinite bound will be replaced by a closed bound
          case _: InfiniteBound[T] => NonEmptyBounds(ClosedBound(value), upper)
        }
      else Bounds.empty //value exceeds upper bound
    }
    else if (isNaN(value)) Bounds.empty
    else this
  }

  def constrainUpper(value: T): Bounds[T] = {
    if (upper.boundsUpper(value)) {
      if (lower.boundsLower(value))
        upper match {
          case _: ClosedBound[T]   => NonEmptyBounds(lower, ClosedBound(value))
          case _: OpenBound[T]     => NonEmptyBounds(lower, OpenBound(value))
          // Note, an upper infinite bound will be replaced by an open bound
          case _: InfiniteBound[T] => NonEmptyBounds(lower, OpenBound(value))
        }
      else Bounds.empty //value exceeds lower bound
    }
    else if (isNaN(value)) Bounds.empty
    else this
  }

  override def toString: String = (lower, upper) match {
    case (ClosedBound(l),  ClosedBound(u))   => s"[$l,$u]"
    case (ClosedBound(l),  OpenBound(u))     => s"[$l,$u)"
    case (OpenBound(l),    ClosedBound(u))   => s"($l,$u]"
    case (OpenBound(l),    OpenBound(u))     => s"($l,$u)"
    case (InfiniteBound(), ClosedBound(u))   => s"(-inf,$u]"
    case (InfiniteBound(), OpenBound(u))     => s"(-inf,$u)"
    case (ClosedBound(l),  InfiniteBound())  => s"[$l,inf)"
    case (OpenBound(l),    InfiniteBound())  => s"($l,inf)"
    case (InfiniteBound(), InfiniteBound())  => s"(-inf,inf)"
  }
}

/** [[Bounds]] that contain a single value */
final case class PointBounds[T] private[util] (point: T)
  (using ord: PartialOrder[T]) extends Bounds[T] {
  def constrainLower(value: T): Bounds[T] =
    if (ord.eqv(point, value)) this else Bounds.empty
  def constrainUpper(value: T): Bounds[T] =
    if (ord.eqv(point, value)) this else Bounds.empty
  override def toString: String = s"[$point]"
}

/** [[Bounds]] that cover nothing */
final case class EmptyBounds[T: PartialOrder] private[util] ()
  extends Bounds[T] {
  def constrainLower(value: T): Bounds[T] = Bounds.empty
  def constrainUpper(value: T): Bounds[T] = Bounds.empty
  override def toString: String = "0"
}

//==== Companion object ====//

object Bounds {

  def empty[T: PartialOrder]: Bounds[T] = new EmptyBounds

  /**
   * Constructs [[Bounds]] with the given lower and upper bounds.
   *
   * If [[inclusive]] is true, both bounds will be inclusive. If false,
   * the lower bound will be inclusive and the upper bound exclusive.
   *
   * If either bound is a NaN, this will return an empty [[Bounds]].
   */
  def of[T](lower: T, upper: T, inclusive: Boolean = false)
    (using ord: PartialOrder[T]): Bounds[T] =
    (lower, upper) match {
      case (l, u) if (isNaN(l) || isNaN(u)) => Bounds.empty
      case (l, u) if (isInf(l) && isInf(u)) => Bounds.unbounded
      case (l, u) if (ord.eqv(l, u)) =>
        if (inclusive) PointBounds(l) else Bounds.empty
      case (l, u) if isInf(l) =>
        val up = if (inclusive) ClosedBound(u) else OpenBound(u)
        NonEmptyBounds(InfiniteBound(), up)
      case (l, u) if (isInf(u)) =>
        NonEmptyBounds(ClosedBound(l), InfiniteBound())
      case (l, u) if (ord.lt(l, u)) =>
        val up = if (inclusive) ClosedBound(u) else OpenBound(u)
        NonEmptyBounds(ClosedBound(l), up)
      case _ => Bounds.empty
    }

  /** Constructs [[Bounds]] with inclusive lower and upper bounds. */
  def inclusive[T: PartialOrder](lower: T, upper: T): Bounds[T] =
    of(lower, upper, true)

  /** Constructs infinite bounds */
  def unbounded[T: PartialOrder]: Bounds[T] =
    NonEmptyBounds(InfiniteBound(), InfiniteBound())

  /** Constructs bounds with closed lower bound and infinite upper bound */
  def closedLower[T: PartialOrder](value: T): Bounds[T] =
    if (isNaN(value))      Bounds.empty
    else if (isInf(value)) Bounds.unbounded
    else NonEmptyBounds(ClosedBound(value), InfiniteBound())

  /** Constructs bounds with open lower bound and infinite upper bound */
  def openLower[T: PartialOrder](value: T): Bounds[T] =
    if (isNaN(value))      Bounds.empty
    else if (isInf(value)) Bounds.unbounded
    else NonEmptyBounds(OpenBound(value), InfiniteBound())

  /** Constructs bounds with infinite lower bound and closed upper bound */
  def closedUpper[T: PartialOrder](value: T): Bounds[T] =
    if (isNaN(value))      Bounds.empty
    else if (isInf(value)) Bounds.unbounded
    else NonEmptyBounds(InfiniteBound(), ClosedBound(value))

  /** Constructs bounds with infinite lower bound and open upper bound */
  def openUpper[T: PartialOrder](value: T): Bounds[T] = {
    if (isNaN(value))      Bounds.empty
    else if (isInf(value)) Bounds.unbounded
    else NonEmptyBounds(InfiniteBound(), OpenBound(value))
  }

  /** Constructs a PointBound with the given value */
  def at[T: PartialOrder](value: T): Bounds[T] = PointBounds(value)
}

//==== Bound ====/

/**
 * [[Bound]] defines a boundary (lower or upper) of a coverage.
 */
sealed trait Bound[T] {
  /** Does this bound the given value on the lower side */
  def boundsLower(value: T): Boolean
  /** Does this bound the given value on the upper side */
  def boundsUpper(value: T): Boolean
}

/** Defines a [[Bound]] that is inclusive. */
final case class ClosedBound[T] private[util] (private val value: T)
(using ord: PartialOrder[T]) extends Bound[T] {
  def boundsLower(v: T): Boolean = ord.lteqv(value, v)
  def boundsUpper(v: T): Boolean = ord.gteqv(value, v)
}

/** Defines a [[Bound]] that is exclusive. */
final case class OpenBound[T] private[util] (private val value: T)
(using ord: PartialOrder[T]) extends Bound[T] {
  def boundsLower(v: T): Boolean = ord.lt(value, v)
  def boundsUpper(v: T): Boolean = ord.gt(value, v)
}

/** Defines a [[Bound]] that does not constrain. */
final case class InfiniteBound[T] private[util] () extends Bound[T] {
  def boundsLower(v: T): Boolean = ! isNaN(v)
  def boundsUpper(v: T): Boolean = ! isNaN(v)
}

//==== Utility methods ====//

private def isNaN(v: Any): Boolean = v match {
  case v: Double => v.isNaN
  case v: Float  => v.isNaN
  case _ => false
}

private def isInf(v: Any): Boolean = v match {
  case v: Double => v.isInfinite
  case v: Float  => v.isInfinite
  case _ => false
}

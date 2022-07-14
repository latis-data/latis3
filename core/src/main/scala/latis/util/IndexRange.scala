package latis.util

import cats.syntax.all._
import fs2.Pure
import fs2.Stream

/**
 * An IndexRange represents the indices defining one dimension of a Section.
 *
 * A FiniteRange is expressed in terms of start, end, and step positive integers.
 * The end value is inclusive and is required to be greater than or equal to the start.
 * The length is limited to Int.MaxValue (2147483647).
 *
 * An UnlimitedRange is expressed in terms of start and step positive integers
 * but has no end. The length is undefined.
 *
 * An EmptyRange represents an IndexRange of length zero.
 */
sealed trait IndexRange {

  /** Returns the number of elements in this IndexRange if it is not unlimited. */
  def length: Option[Int] = this match {
    case FiniteRange(start, end, step) => Some((end - start) / step  + 1)
    case _: UnlimitedRange             => None
    case EmptyRange                    => Some(0)
  }

  /** Returns whether this IndexRange has a specified length. */
  def isUnlimited: Boolean = length.isEmpty

  /** Returns whether this IndexRange is empty. */
  def isEmpty: Boolean = length.contains(0)

  /** Computes the new step size given the original step and a new stride. */
  private def computeNewStep(origStep: Int, stride: Int): Either[LatisException, Int] = {
    if (stride > Int.MaxValue / origStep) LatisException("New step exceeds max Int").asLeft
    else if (stride <= 0) LatisException("stride must be greater than zero").asLeft
    else (origStep * stride).asRight
  }

  /**
   * Applies a stride to this IndexRange.
   *
   * Note that this takes the current stride into account, unlike scala's Range.by.
   * The current end value may be truncated to match a whole number of steps.
   */
  def stride(stride: Int): Either[LatisException, IndexRange] =
    if (stride == 1) this.asRight //no-op
    else this match {
      case FiniteRange(start, end, step) =>
        computeNewStep(step, stride).flatMap(IndexRange.finite(start, end, _))
      case UnlimitedRange(start, step) =>
        computeNewStep(step, stride).map(UnlimitedRange(start, _))
      case EmptyRange => EmptyRange.asRight
    }

  /**
   * Creates a subset of this IndexRange.
   *
   * The start and end values are inclusive but they may be truncated
   * to align with the current start and step and to preserve whole steps.
   */
  def subset(from: Int, to: Int, stride: Int = 1): Either[LatisException, IndexRange] = {
    if (from < 0) LatisException("Subset start must be non-negative").asLeft
    else if (to < from) LatisException("Subset end must not be less that start").asLeft
    else if (stride <= 0) LatisException("Subset stride must greater than zero").asLeft
    else this match {
      case FiniteRange(start, end, step) =>
        computeNewStep(step, stride).flatMap { newStep =>
          val newStart =
            if (start >= from) start
            // Get first value in range >= "from" accounting for step
            else Math.ceil((from - start).toDouble / step).toInt * step + start
          val newEnd = Math.min(end, to)
          if (newStart > newEnd) EmptyRange.asRight
          else IndexRange.finite(newStart, newEnd, newStep)
        }
      case UnlimitedRange(start, step) =>
        computeNewStep(step, stride).flatMap { newStep =>
          val newStart =
            if (start >= from) start
            // Get first value in range >= "from" accounting for step
            else Math.ceil((from - start).toDouble / step).toInt * step + start
          if (newStart > to) EmptyRange.asRight
          else IndexRange.finite(newStart, to, newStep)
        }
      case EmptyRange => EmptyRange.asRight
    }
  }

  /**
   * Presents this IndexRange using range notation.
   *
   *   FiniteRange: "start:end:step"
   *   UnlimitedRange: "start:*:step"
   *   EmptyRange: "0" (i.e. a length of zero)
   *
   * These values can in turn be used by IndexRange.fromExpression.
   */
  override def toString: String = this match {
    case FiniteRange(start, end, step) => s"$start:$end:$step"
    case UnlimitedRange(start, step)   => s"$start:*:$step"
    case EmptyRange                    => "0" //represents length = 0
  }
}

sealed trait NonEmptyRange extends IndexRange {
  //TODO: consider putting other methods here, e.g. stride
  //  Section has only NonEmptyRanges, this preserves the type so we can use the constructor safely
  //  But no reason to exclude these from EmptyRange?

  /** Breaks this IndexRange into a Stream of FiniteRanges with the given length. */
  def chunk(size: Int): Stream[Pure, NonEmptyRange] = {
    //Note: treating size < 1 as no-op: one single chunk //TODO: consider error, refined type
    if (size < 1) Stream(this) //single chunk
    else this match {
      case FiniteRange(start, end, step) =>
        Stream.emits(start to end by size * step).map { s =>
          FiniteRange(s, Math.min(s + (size - 1) * step, end), step)
        }
      case UnlimitedRange(start, step) =>
        Stream.iterate(start)(_ + size * step).map { s =>
          FiniteRange(s, s + (size - 1) * step, step)
        }
    }
  }
}

/** Represents an IndexRange with a specified end. */
final case class FiniteRange private[util] (start: Int, end: Int, step: Int) extends NonEmptyRange

/** Represents an IndexRange without a specified end. */
final case class UnlimitedRange private (start: Int, step: Int) extends NonEmptyRange

/** Represents an empty IndexRange. */
object EmptyRange extends IndexRange


object IndexRange {

  /**
   * Constructs a FiniteRange.
   *
   * The inclusive end value may be truncated to match a whole number of steps.
   * This ensures that `end` is indeed the last value.
   */
  def finite(from: Int, to: Int, stride: Int = 1): Either[LatisException, FiniteRange] = {
    if (from < 0)        LatisException("IndexRange start must be non-negative").asLeft
    else if (from > to)  LatisException("IndexRange end must be greater than start").asLeft
    else if (stride < 1) LatisException("IndexRange step must be greater than zero").asLeft
    else {
      // Note: integer division will do floor
      val end = (to - from) / stride * stride + from
      FiniteRange(from, end, stride).asRight
    }
  }

  /** Constructs an UnlimitedRange. */
  def unlimited(from: Int, stride: Int): Either[LatisException, UnlimitedRange] = {
    if (from < 0)        LatisException("IndexRange start must be non-negative").asLeft
    else if (stride < 1) LatisException("IndexRange step must be greater than zero").asLeft
    else UnlimitedRange(from, stride).asRight
  }

  /**
   * Constructs an IndexRange from a range expression.
   *
   * An IndexRange expression is a string representation of an index range of the forms:
   *   "start:end:step"
   *   "start:end" - Equivalent to "start:end:1"
   *   "length" - Equivalent to "0:length-1:1", must be >= 0
   *
   * All values must be non-negative integers.
   * The end value is inclusive or may be "*" to indicate an unlimited range.
   *
   * The output of toString adheres to this expression syntax.
   */
  def fromExpression(expr: String): Either[LatisException, IndexRange] =
    expr.split(":").toList match {
      case length :: Nil => parseInt(length).flatMap { l =>
        if (l < 0) LatisException("Length must be greater than zero.").asLeft
        else if (l == 0) EmptyRange.asRight
        else FiniteRange(0, l - 1, 1).asRight
      }
      case start :: end :: Nil => for {
        s <- parseInt(start)
        e <- parseEnd(end)
        r <- makeRange(s, e)
      } yield r
      case start :: end :: step :: Nil => for {
        s <- parseInt(start)
        e <- parseEnd(end)
        d <- parseInt(step)
        r <- makeRange(s, e, d)
      } yield r
      case _ => LatisException(s"Invalid range expression: $expr").asLeft
    }

  /** Adds special handling for end = -1 for unlimited range. */
  private def makeRange(start: Int, end: Int, step: Int = 1): Either[LatisException, IndexRange] = {
    // Delegates to primary constructors for error handling.
    if (end == -1) IndexRange.unlimited(start, step)
    else IndexRange.finite(start, end, step)
  }

  /** Parses a string as an Int. */
  private def parseInt(s: String): Either[LatisException, Int] =
    Either.catchOnly[NumberFormatException](s.toInt).leftMap(LatisException(_))

  /**
   * Parses a string representing the upper value of a IndexRange.
   *
   * A "*" will yield "-1", representing an unlimited range.
   */
  private def parseEnd(end: String): Either[LatisException, Int] = end match {
    case "*" => (-1).asRight
    case end => parseInt(end)
  }

  /** Returns an empty IndexRange. */
  def empty: IndexRange = EmptyRange
}

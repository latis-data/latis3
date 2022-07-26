package latis.util

import cats.syntax.all._
import fs2.Pure
import fs2.Stream

/**
 * A Section supports multi-dimensional array algebra by managing the indices
 * of a multi-dimensional Cartesian domain set.
 *
 * The length of each dimension is limited to Int.MaxValue (2147483647).
 * The overall length (product of the dimension lengths) is a Long.
 * The length may be undefined and logically unlimited, thus the use of Option.
 * The length of an undefined dimension will be represented by -1 in the shape.
 *
 * Only the first dimension can be unlimited. This allows LaTiS to stream
 * an arbitrarily large dataset without knowing how many samples exist. This
 * unlimited dimension is effectively limited to Int.MaxValue since subsetting
 * uses Int indices. Inner dimensions must have a fixed length to support
 * Cartesian indexing logic.
 */
sealed abstract case class Section private (ranges: List[NonEmptyRange]) {

  /**
   * Returns the total number of elements represented by this Section
   * if the first dimension length is not unlimited.
   */
  def length: Option[Long] = shape match {
    case Nil       => Some(0)
    case -1 :: Nil => None
    case _ => Some(shape.foldLeft(1L)(_ * _))
  }

  /** Returns the number of dimensions in this Section. */
  def rank: Int = ranges.length

  /**
   * Returns a List of dimension lengths.
   *
   * A "-1" indicates an unlimited dimension length.
   * Only the the first dimension may be unlimited.
   */
  def shape: List[Int] = ranges.map {
    case r if r.isUnlimited => -1
    case r                  => r.length.get
  }

  /** Returns whether this is an empty Section. */
  def isEmpty: Boolean = ranges.isEmpty

  /** Returns a new Section with the first element of each dimension. */
  def head: Section = {
    val newRanges = ranges.map {
      case FiniteRange(start, _, _) => FiniteRange(start, start, 1)
      case UnlimitedRange(start, _) => FiniteRange(start, start, 1)
    }
    Section(newRanges)
  }

  //TODO: last, only if not unlimited
  //TODO: take? needs to be multiple of inner dimensions, round up? just use iterator or stream?

  /** Returns the IndexRange for the given dimension. */
  def range(dim: Int): Either[LatisException, NonEmptyRange] =
    Either.fromOption(ranges.lift(dim), LatisException(s"Invalid dimension: $dim"))

  /** Returns a new Section with a stride applied to the given dimension. */
  def strideDimension(dim: Int, stride: Int): Either[LatisException, Section] =
    if (isEmpty) this.asRight
    else for {
      range    <- range(dim)
      newRange <- range.stride(stride)
      section  <- Section.fromRanges(ranges.updated(dim, newRange))
    } yield section

  /** Returns a new Section with a multi-dimensional stride applied to this Section. */
  def stride(strides: Seq[Int]): Either[LatisException, Section] =
    if (isEmpty) this.asRight
    else if (strides.length != rank)
      LatisException("Number of stride elements must match the Section rank").asLeft
    else ranges.zip(strides).traverse {
      case (r, s) => r.stride(s)
    }.flatMap(Section.fromRanges)

  /**
   * Breaks this Section into a Stream of contiguous Sections with the target number of elements.
   *
   * This will break the Section along the outer dimension so the actual size
   * of a chunk may be greater than the target size but no more than double.
   */
   def chunk(size: Int): Stream[Pure, Section] =
     //TODO: deal with size <= 0, don't raise error in stream?
     if (isEmpty) Stream.empty
     else {
       // Note: product of empty list is 1
       val n = Math.ceil(size.toDouble / shape.tail.product).toInt
       ranges.head.chunk(n).map { r =>
         Section(r :: ranges.tail)
       }
     }

  /** Returns a new Section with a subset applied to the given dimension. */
  def subsetDimension(dim: Int, from: Int, to: Int, stride: Int = 1): Either[LatisException, Section] =
    if (isEmpty) this.asRight
    else for {
      range    <- range(dim)
      newRange <- range.subset(from, to, stride)
      section  <- Section.fromRanges(ranges.updated(dim, newRange))
    } yield section

  //TODO: intersect(section)?

  /**
   * A subset expression (a.k.a. hyperslab notation) is a
   * comma separated list of Range subset expressions.
   * Range subset expression:
   *  - "start:end:step"
   *  - "start:end" - Equivalent to "start:end:1"
   *  - "slice" - Equivalent to "slice:slice:1"
   *  - ":" - all
   * The end values are inclusive.
   */
  //TODO: def subset(expr: String): Section = ???

  /** Defines equality in terms of the string representation. */
  override def equals(obj: Any): Boolean = obj match {
    case s: Section => s.toString() == this.toString()
    case _ => false
  }

  /** Defines the hash code to be consistent with toString. */
  override def hashCode(): Int = 37 + toString().hashCode()

  /**
   * Represents this Section as a comma separated list of IndexRange expressions.
   *
   * This form is suitable for netcdf-java Section construction.
   * This will return "empty" for an empty Section.
   */
  override def toString: String =
    if (isEmpty) "empty"
    else ranges.map(_.toString).mkString(",")
}

object Section {

  private def apply(ranges: List[NonEmptyRange]): Section =
    new Section(ranges) {}

  /**
   * Returns an empty Section
   *
   * An empty Section is represented with an empty list of ranges.
   */
  def empty: Section = Section(List.empty)

  /**
   * Constructs a Section given a list of IndexRanges representing each dimension.
   *
   * The first dimension may be unlimited. If an IndexRange in empty, the resulting Section
   * will be empty. An empty list of ranges denotes an empty Section.
   */
  def fromRanges(ranges: List[IndexRange]): Either[LatisException, Section] =
    if (ranges.isEmpty || ranges.exists(_.isEmpty)) Section.empty.asRight
    else if (ranges.tail.exists(_.isUnlimited))
      LatisException("Only the first dimension of a range can be unlimited.").asLeft
    else Section(ranges.collect { case ner: NonEmptyRange => ner }).asRight

  /**
   * Constructs a Section given the size of each dimension.
   *
   * The resulting ranges will start at zero and have a step of one.
   * A negative one for the first dimension will be interpreted as unlimited.
   * The length of other dimensions must be greater than one. An empty shape
   * will result in an empty Section.
   */
  def fromShape(shape: List[Int]): Either[LatisException, Section] = {
    if (shape.isEmpty) Section.empty.asRight
    else if (shape.contains(0)) Section.empty.asRight
    else if (shape.tail.exists(_ < 0))
      LatisException("Inner dimensions must have non-negative length.").asLeft
    else if (shape.head < -1)
      LatisException("First dimension must have length of -1 (unlimited) or non-negative.").asLeft
    else Section(
      shape.map {
        case -1 => UnlimitedRange(0, 1)
        case n  => FiniteRange(0, n - 1, 1)
      }).asRight
  }

  /**
   * Constructs a Section from a comma separated list of IndexRange expressions.
   *
   * Only the first dimension may be unlimited. If any IndexRange is empty,
   * or the expression itself is empty, the resulting Section will be empty.
   */
  def fromExpression(expr: String): Either[LatisException, Section] =
    if (expr.isEmpty) Section.empty.asRight
    else expr.split(",").toList.traverse(IndexRange.fromExpression).flatMap(fromRanges)
      .leftMap(e => LatisException(s"Invalid Section expression: $expr", e))

}

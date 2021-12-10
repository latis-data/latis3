package latis.util

import cats.effect.IO
import cats.syntax.all._
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
 * Only the first dimension can have an undefined length. This allows LaTiS to stream
 * an arbitrarily large dataset without knowing how many samples exist. This
 * unlimited dimension is effectively limited to Int.MaxValue since subsetting
 * uses Int indices. Inner dimensions must have a fixed length to support
 * Cartesian indexing logic.
 */
class Section private (ranges: List[Range.Inclusive]) {
  //TODO: use NonEmptyList, or do we want to support empty Section?
  /*
   * Developer notes on internal implementation:
   *
   * The internal state of a Section is managed with Scala's Range.Inclusive.
   * Inclusivity goes against the general treatment of ranges in LaTiS but this
   * is consistent with netCDF's ucar.ma2.Range and OPeNDAP's hyperslab notation.
   * Note that Range.Exclusive is the default Range so be careful about off-by-one
   * errors. The use of Range is entirely encapsulated to ensure self-consistency.
   *
   * An unlimited dimension is represented by a Range(0, -1) which is otherwise
   * prevented since it will be considered empty. This allows us to manage a step
   * on an unlimited dimension.
   *
   * It's tempting to use our own implementation of Range since our use case
   * does not align as well to Scala's Range:
   * - there is no natural way to represent an unlimited range
   * - we need to be careful about inclusivity logic
   * - we always want start <= end and step > 0
   * - by(step) simply replaces the step while we want it to multiply the current step
   * - intersect has the behavior we want but returns an IndexedSeq
   */

  /**
   * Returns the total number of elements represented by this Section
   * if the first dimension length is not undefined.
   */
  def length: Option[Long] = shape match {
    case -1 :: Nil => None
    case _ => Some(shape.foldLeft(1L)(_ * _))
  }

  /**
   * Returns the number of dimensions in this Section.
   */
  def rank: Int = ranges.length

  /**
   * Returns a List of dimension lengths.
   * A "-1" indicates an undefined and potentially unlimited dimension length.
   * Only the the first dimension may be undefined.
   */
  def shape: List[Int] = ranges.map {
    case r: Range if r.isEmpty => -1
    case r: Range              => r.length
  }

  /** Returns a new Section with the first element of each dimension. */
  def head: Section = {
    val newRanges = ranges.map { r =>
      val i = r.start
      Range.inclusive(i,i)
    }
    new Section(newRanges)
  }

  //TODO: last, only if not unlimited
  //TODO: take? needs to be multiple of inner dimensions, round up? just use iterator or stream?

  /** Returns a 1D Section for the given dimension. */
  def slice(dim: Int): Either[LatisException, Section] =
    ranges.lift(dim).map(r => Section.fromRanges(List(r)))
      .getOrElse(LatisException(s"Invalid dimension: $dim").asLeft)

  /** Returns a new Section with a stride applied to the given dimension. */
  def strideDimension(dim: Int, step: Int): Either[LatisException, Section] =
    for {
      range <- getDimensionRange(dim)
      newRange <- Either.cond(
        step > 0,
        Range.inclusive(range.start, range.end, range.step * step),
        LatisException("Step must be greater than zero.")
      )
    } yield new Section(ranges.updated(dim, newRange))

  /** Returns a new Section with a multi-dimensional stride applied to this Section. */
  def stride(stride: Seq[Int]): Either[LatisException, Section] = {
    if (stride.length != rank)
      LatisException("Number of stride elements must match the Section rank").asLeft
    else Either.cond(
      stride.forall(_ > 0),
      ranges.zip(stride).map {
        case (r, s) => Range.inclusive(r.start, r.end, r.step * s)
      },
      LatisException("Stride elements must be greater than zero")
    ).map(rs => new Section(rs))
  }

  /**
   * Break the Section into contiguous Sections with the target number of elements.
   *
   * This will break the Section along the outer dimension so the actual size
   * of a chunk may be greater than the target size
   */
   def chunk(size: Int): Stream[IO, Section] = {
     //TODO: support unlimited dimension
     if (size <= 0) Stream.raiseError[IO](LatisException("Chunk size must be greater than zero."))
     else {
       val n = Math.ceil(size.toDouble / shape.tail.product).toInt
       Stream.emits(ranges.head.grouped(n).toList).map {
         case r: Range.Inclusive => new Section(r +: ranges.tail)
         case _ => ??? //only if Range.Inclusive.grouped fails to preserve Range.Inclusive
       }
     }
   }

  /** Returns a new Section with a subset applied to the given dimension. */
  def subsetDimension(dim: Int, from: Int, to: Int, step: Int = 1): Either[LatisException, Section] =
    for {
      range <- getDimensionRange(dim)
      newStart = Math.max(range.start, from)
      newEnd = if (range.isEmpty) to else Math.min(range.end, to)
      newStep <- Either.cond(
        step > 0,
        range.step * step,
        LatisException("Step must be greater than zero.")
      )
      newRange = Range.inclusive(newStart, newEnd, newStep)
    } yield new Section(ranges.updated(dim, newRange))

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

  /** Returns the Range for the given dimension. */
  private def getDimensionRange(dim: Int): Either[LatisException, Range.Inclusive] =
    ranges.lift(dim).toRight(LatisException(s"No dimension $dim for rank $rank Section."))

  override def equals(obj: Any): Boolean = obj match {
    case s: Section => s.toString() == this.toString()
    case _ => false
  }

  override def hashCode(): Int = toString().hashCode()

  override def toString(): String = ranges.map { r =>
    s"${r.start}:${r.end}:${r.step}"
  }.mkString(",").replace("-1", "*")
}

object Section {

  /**
   * Constructs a Section given a list of ranges representing each dimension.
   * Ranges must be non-empty except the first when representing an unlimited
   * dimension.
   * Note that a Range will be considered empty if end < start.
   */
  private def fromRanges(ranges: List[Range.Inclusive]): Either[LatisException, Section] = ranges match {
    case Nil      => LatisException("Section must have at least one range").asLeft
    case _ :: Nil => new Section(ranges).asRight
    case r0 :: rs =>
      Either.cond (
        rs.forall (_.nonEmpty),
        new Section (r0 +: rs),
        LatisException("A Section may not have an empty Range.")
     )
  }

  /**
   * Constructs a Section given the size of each dimension.
   */
  def fromShape(shape: Seq[Int]): Section =
    new Section(shape.toList.map(n => Range.inclusive(0, n - 1)))

  /**
   * The Section expression is a comma separated list of Range expressions.
   * Range expression:
   *   "start:end:step"
   *   "start:end" - Equivalent to "start:end:1"
   *   "length" - Equivalent to "0:length-1:1", must be > 0
   * where all values are non-negative integers.
   * The end values are inclusive.
   * The "end" value may be "*" to indicate an unlimited dimension.
   * Only the first dimension may be unlimited.
   */
  def fromExpression(expr: String): Either[LatisException, Section] =
    expr.split(",").toList.traverse { rangeExpr =>
      rangeExpr.split(":").toList match {
        case length :: Nil => for {
          len <- parseInt(length).flatMap { l =>
            Either.cond(
              l > 0,
              l,
              LatisException("Length must be greater than zero.")
            )
          }
        } yield Range.inclusive(0, len - 1)
        case start :: end :: Nil => for {
          s <- parseInt(start)
          e <- parseEnd(end)
        } yield Range.inclusive(s, e)
        case start :: end :: step :: Nil => for {
          s <- parseInt(start)
          e <- parseEnd(end)
          d <- parseInt(step)
        } yield Range.inclusive(s, e, d)
        case _ => LatisException(s"Invalid range expression: $rangeExpr").asLeft
      }
    }.flatMap { ranges =>
      fromRanges(ranges)
    }.leftMap { t =>
      LatisException(s"Invalid Section expression: $expr", t)
    }

  /** Parses a string as a non-negative Int. */
  private def parseInt(s: String): Either[Exception, Int] =
    Either.catchOnly[NumberFormatException](s.toInt).flatMap { n =>
      Either.cond(
        n >= 0,
        n,
        LatisException("Range value must be non-negative.")
      )
    }

  /**
   * Parses a string as an Int. A "*" will yield "-1".
   * This is used to support unlimited dimensions.
   */
  private def parseEnd(end: String): Either[Exception, Int] = end match {
    case "*" => (-1).asRight
    case _   => parseInt(end)
  }
}

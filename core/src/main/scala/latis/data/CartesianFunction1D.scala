package latis.data

import scala.collection.Searching.Found
import scala.collection.Searching.SearchResult

import cats.syntax.all._

import latis.util.DataUtils._
import latis.util.DefaultDomainOrdering
import latis.util.LatisException

/**
 * Manages a one-dimensional SampledFunction as sequences of Data.
 * This requires IndexedSeq which provides near constant-time element
 * access and length computation. The range data could be any Data type
 * while the domain data must be Datum representing Scalar variables.
 * The smart constructors ensure that sequences have consistent lengths
 * and types.
 */
//TODO: prevent direct construction?
class CartesianFunction1D(
  val xs: IndexedSeq[Datum],
  val vs: IndexedSeq[RangeData],
  val ordering: Option[PartialOrdering[DomainData]]
  //val interpolation: Option[Interpolation] = None
) extends CartesianFunction {
  //Note, using Seq instead of invariant Array to get variance
  // requires IndexedSeq, promises fast indexing

  private def applySearchResults(sr: SearchResult): Either[LatisException, RangeData] =
    sr match {
      case Found(i) => Right(vs(i))
      //TODO: interp
      //case InsertionPoint(i) =>
      //if (i >= vs.length) Right(vs(i-1))
      //else Right(vs(i))
      case _ =>
        val msg = "Evaluation failed." //replace with interp error
        Left(LatisException(msg))
    }

  override def apply(data: DomainData): Either[LatisException, RangeData] = data match {
    case DomainData(x: Datum) =>
      for {
        sr    <- searchDomain(0, xs, x)
        range <- applySearchResults(sr)
      } yield range
    case _ =>
      val msg = "The Data argument must be a Datum"
      Left(LatisException(msg))
  }

  /**
   * Provides a sequence of samples to fulfill the MemoizedFunction trait.
   */
  def sampleSeq: Seq[Sample] =
    (xs.zip(vs)).map { case (x, v) => Sample(DomainData(x), RangeData(v)) }

}

//==============================================================================

object CartesianFunction1D {
  //TODO: extend FunctionFactory, return Either

  def fromData(
    xs: IndexedSeq[Datum],
    vs: IndexedSeq[Data],
    ordering: Option[PartialOrdering[DomainData]] = None
  ): Either[LatisException, CartesianFunction1D] = {
    // Validates that the domain and range variable Seqs have the same length
    val nx = xs.length
    val nv = vs.length
    if (nx != nv) {
      val msg = s"Domain and Range lengths don't match: $nx, $nv"
      Left(LatisException(msg))
    } else {
      Right(new CartesianFunction1D(xs, vs.map(RangeData(_)), ordering))
    }
  }

  /**
   * Tries to construct a CartesianFunction1D given sequences of data values
   * of any type.
   * This asserts that the Seq lengths are consistent, that the data values
   * correspond to a supported ValueType, and that each element in a Seq
   * has the same type.
   */
  //TODO: will implicit value classes make this obsolete?
  def fromValues(xs: Seq[Any], vs: Seq[Any]*): Either[LatisException, CartesianFunction1D] = {
    // Ensures that data Seqs are not empty
    //TODO: allow empty domain?
    if (xs.isEmpty || vs.isEmpty) {
      val msg = "Value sequences must not be empty."
      return Left(LatisException(msg))
    }

    // Validates length of each range variable Seq
    val nv = vs.head.length
    val ls = vs.map(_.length)
    if (!ls.tail.forall(_ == nv)) {
      val msg = s"All range variable sequences must be the same length: ${ls.mkString(",")}"
      return Left(LatisException(msg))
    }

    // Validates that the domain and range variable Seqs have the same length
    val nx = xs.length
    if (nx != nv) {
      val msg = s"Domain and Range lengths don't match: $nx, $nv"
      return Left(LatisException(msg))
    }

    // Combines the data into a CartesianFunction
    for {
      d1s <- anySeqToDatumSeq(xs)
      rss <- vs.toVector.traverse(anySeqToDatumSeq)
      rs = rss.transpose.map(Data.fromSeq(_)).map(RangeData(_))
    } yield new CartesianFunction1D(d1s.toIndexedSeq, rs, Some(DefaultDomainOrdering))
  }

}

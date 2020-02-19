package latis.data

import scala.collection.Searching.Found

import cats.implicits._

import latis.util.DataUtils._
import latis.util.LatisException

/**
 * Manages a two-dimensional SampledFunction as sequences of Data.
 * This requires IndexedSeq which provides near constant-time element
 * access and length computation. The range data could be any Data type
 * while the domain data must be Datum representing Scalar variables.
 * The smart constructors ensure that sequences have consistent lengths
 * and types.
 */
class CartesianFunction2D(
  val xs: IndexedSeq[Datum],
  val ys: IndexedSeq[Datum],
  val vs: IndexedSeq[IndexedSeq[RangeData]]
) extends CartesianFunction {
  //TODO: require smart constructor to provide validation?

  override def apply(data: DomainData): Either[LatisException, RangeData] = data match {
    case DomainData(x: Datum, y: Datum) =>
      (searchDomain(xs, x), searchDomain(ys, y)) match {
        case (Found(i), Found(j)) => Right(vs(i)(j))
        //TODO: interpolate
        case _ =>
          val msg = s"No sample found matching $data"
          Left(LatisException(msg))
      }
    case _ =>
      val msg = s"Invalid evaluation value for IndexedFunction2D: $data"
      Left(LatisException(msg))
  }

  /**
   * Provide a sequence of samples to fulfill the MemoizedFunction trait.
   */
  def sampleSeq: Seq[Sample] =
    for {
      ia <- xs.indices
      ib <- ys.indices
    } yield Sample(DomainData(xs(ia), ys(ib)), RangeData(vs(ia)(ib)))
}

object CartesianFunction2D {
  //TODO: FF fromSamples

  /**
   * Tries to construct a CartesianFunction2D given sequences of data values
   * of any type.
   * This asserts that the Seq lengths are consistent, that the data values
   * correspond to a supported ValueType, and that each element in a Seq
   * has the same type.
   */
  def fromValues(
    xs: Seq[Any],
    ys: Seq[Any],
    vs: Seq[Seq[Any]]*
  ): Either[LatisException, CartesianFunction2D] = {
    // Ensures that data Seqs are not empty
    if (xs.isEmpty || ys.isEmpty || vs.isEmpty) {
      val msg = "Value sequences must not be empty."
      return Left(LatisException(msg))
    }

    // Validates length of each range variable Seq
    val nv = vs.head.flatten.length
    val ls = vs.map(_.flatten.length)
    if (!ls.tail.forall(_ == nv)) {
      val msg = s"All range variable sequences must be the same length: ${ls.mkString(",")}"
      return Left(LatisException(msg))
    }

    // Validates that the domain and range variable Seqs have the same length
    val nx = xs.length
    val ny = ys.length
    if (nx * ny != nv) {
      val msg = s"Domain and Range lengths don't match: ($nx x $ny), $nv"
      return Left(LatisException(msg))
    }

    for {
      d1s <- anySeqToDatumSeq(xs)
      d2s <- anySeqToDatumSeq(ys)
      rss <- vs.map(_.flatten).toVector.traverse(anySeqToDatumSeq)
      rs: IndexedSeq[IndexedSeq[RangeData]] = rss.transpose.map(RangeData(_)).grouped(ny).toIndexedSeq
    } yield new CartesianFunction2D(d1s.toIndexedSeq, d2s.toIndexedSeq, rs)
  }
}

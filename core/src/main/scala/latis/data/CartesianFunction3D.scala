package latis.data

import scala.collection.Searching.Found
import scala.collection.Searching.SearchResult

import cats.syntax.all._

import latis.util.DataUtils.anySeqToDatumSeq
import latis.util.DefaultDomainOrdering
import latis.util.LatisException

/**
 * Manages a three-dimensional SampledFunction as sequences of Data.
 * This requires IndexedSeq which provides near constant-time element
 * access and length computation. The range data could be any Data type
 * while the domain data must be Datum representing Scalar variables.
 * The smart constructors ensure that sequences have consistent lengths
 * and types.
 */
class CartesianFunction3D(
  val xs: IndexedSeq[Datum],
  val ys: IndexedSeq[Datum],
  val zs: IndexedSeq[Datum],
  val vs: IndexedSeq[IndexedSeq[IndexedSeq[RangeData]]],
  val ordering: Option[PartialOrdering[DomainData]]
) extends CartesianFunction {
  //TODO: require smart constructor to provide validation?

  override def eval(data: DomainData): Either[LatisException, RangeData] = data match {
    case DomainData(x: Datum, y: Datum, z: Datum) =>
      for {
        sr1   <- searchDomain(0, xs, x)
        sr2   <- searchDomain(1, ys, y)
        sr3   <- searchDomain(2, zs, z)
        range <- applySearchResults(sr1, sr2, sr3)
      } yield range
    case _ =>
      val msg = s"Invalid evaluation value for CartesianFunction3D: $data"
      Left(LatisException(msg))
  }

  private def applySearchResults(
    sr1: SearchResult,
    sr2: SearchResult,
    sr3: SearchResult
  ): Either[LatisException, RangeData] =
    (sr1, sr2, sr3) match {
      case (Found(i), Found(j), Found(k)) => Right(vs(i)(j)(k))
      //TODO: interp
      case _ =>
        val msg = "Evaluation failed." //replace with interp error
        Left(LatisException(msg))
    }

  /**
   * Provides a sequence of samples to fulfill the MemoizedFunction trait.
   */
  def sampleSeq: Seq[Sample] =
    for {
      ia <- xs.indices
      ib <- ys.indices
      ic <- zs.indices
    } yield Sample(DomainData(xs(ia), ys(ib), zs(ic)), RangeData(vs(ia)(ib)(ic)))
}

//==============================================================================

object CartesianFunction3D {
  //TODO: FF fromSamples

  /**
   * Constructs a CartesianFunction3D given sequences of data values
   * of any type.
   * This asserts that the Seq lengths are consistent, that the data values
   * correspond to a supported ValueType, and that each element in a Seq
   * has the same type.
   */
  def fromValues(
    xs: Seq[Any],
    ys: Seq[Any],
    zs: Seq[Any],
    vs: Seq[Seq[Seq[Any]]]*
  ): Either[LatisException, CartesianFunction3D] = {
    // Ensures that data Seqs are not empty
    if (xs.isEmpty || ys.isEmpty || zs.isEmpty || vs.isEmpty) {
      val msg = "Value sequences must not be empty."
      return Left(LatisException(msg))
    }

    // Validates length of each range variable Seq
    val nv = vs.head.flatMap(_.flatten).length
    val ls = vs.map(_.flatMap(_.flatten).length)
    if (!ls.tail.forall(_ == nv)) {
      val msg = s"All range variable sequences must be the same length: ${ls.mkString(",")}"
      return Left(LatisException(msg))
    }

    // Validates that the domain and range variable Seqs have the same length
    val nx = xs.length
    val ny = ys.length
    val nz = zs.length
    if (nx * ny * nz != nv) {
      val msg = s"Domain and Range lengths don't match: ($nx x $ny x $nz), $nv"
      return Left(LatisException(msg))
    }

    // Combines the data into a CartesianFunction
    for {
      d1s <- anySeqToDatumSeq(xs)
      d2s <- anySeqToDatumSeq(ys)
      d3s <- anySeqToDatumSeq(zs)
      rss <- vs.map(xs => xs.flatMap(_.flatten)).toVector.traverse(anySeqToDatumSeq)
      rs: IndexedSeq[IndexedSeq[IndexedSeq[RangeData]]] = rss.transpose
        .map(RangeData(_))
        .grouped(nz)
        .toIndexedSeq
        .grouped(ny)
        .toIndexedSeq
    } yield new CartesianFunction3D(
      d1s.toIndexedSeq,
      d2s.toIndexedSeq,
      d3s.toIndexedSeq,
      rs,
      Some(DefaultDomainOrdering)
    )
  }
}

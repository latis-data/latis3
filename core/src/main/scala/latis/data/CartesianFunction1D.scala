package latis.data

import scala.collection.Searching.Found

import cats.implicits._

import latis.model.ValueType
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
  val vs: IndexedSeq[RangeData]
) extends CartesianFunction {
  //Note, using Seq instead of invariant Array to get variance
  //TODO: require IndexedSeq? promises fast indexing
  //TODO: prevent diff types of Datum within a Seq, do we want invarience?

  override def apply(data: DomainData): Either[LatisException, RangeData] = data match {
    case DomainData(d: Datum) =>
      searchDomain(xs, d) match {
        case Found(i) => Right(vs(i))
        //case InsertionPoint(i) =>
        //  //TODO: interp
        //  if (i >= vs.length) Right(vs(i-1))
        //  else Right(vs(i))
        case _ =>
          val msg = s"No sample found matching $data"
          Left(LatisException(msg))
      }
    //TODO: allow TupleData with one element? disallow direct TupleData construction? Use Data.fromSeq
    case _ =>
      val msg = "The Data argument must be a Datum"
      Left(LatisException(msg))
  }

  /**
   * Provide a sequence of samples to fulfill the MemoizedFunction trait.
   */
  def sampleSeq: Seq[Sample] =
    (xs zip vs) map { case (x, v) => Sample(DomainData(x), RangeData(v)) }

}

object CartesianFunction1D {
  //TODO: extend FunctionFactory, return Either

  def fromData(xs: IndexedSeq[Datum], vs: IndexedSeq[Data]): Either[LatisException, CartesianFunction1D] = {
    // Validates that the domain and range variable Seqs have the same length
    val nx = xs.length
    val nv = vs.length
    if (nx != nv) {
      val msg = s"Domain and Range lengths don't match: $nx, $nv"
      Left(LatisException(msg))
    } else {
      Right(new CartesianFunction1D(xs, vs.map(RangeData(_))))
    }
  }

  /**
   * Tries to construct a CartesianFunction1D given sequences of data values
   * of any type.
   * This asserts that the Seq lengths are consistent, that the data values
   * correspond to a supported ValueType, and that each element in a Seq
   * has the same type.
   */
  def fromValues(xs: Seq[Any], vs: Seq[Seq[Any]]): Either[LatisException, CartesianFunction1D] = {
    // Ensures that data Seqs are not empty
    if (xs.isEmpty || vs.isEmpty) {
      val msg = "Value sequences must not be empty."
      return Left(LatisException(msg))
    }

    // Validates length of each range variable Seq
    val nv = vs.head.length
    val ls = vs.map(_.length)
    if (!ls.forall(_ == nv)) {
      val msg = s"All range variable sequences must be the same length: ${ls.mkString(",")}"
      return Left(LatisException(msg))
    }

    // Validates that the domain and range variable Seqs have the same length
    val nx = xs.length
    if (nx != nv) {
      val msg = s"Domain and Range lengths don't match: $nx, $nv"
      return Left(LatisException(msg))
    }

    for {
      d1s <- anySeqToDatumSeq(xs)
      rss <- vs.toVector.traverse(anySeqToDatumSeq)
      rs = rss.transpose.map(Data.fromSeq(_))
    } yield new CartesianFunction1D(d1s, rs.map(RangeData(_)))
  }


  //TODO util?
  def anySeqToDatumSeq(vs: Seq[Any]): Either[LatisException, IndexedSeq[Datum]] = vs.length match {
    case 0 => Right(IndexedSeq.empty)
    case _ =>
      // Makes sure each element has the same type
      ValueType.fromValue(vs.head).flatMap { vtype =>
        vs.toVector.traverse(v => vtype.makeDatum(v))
      }
  }
}

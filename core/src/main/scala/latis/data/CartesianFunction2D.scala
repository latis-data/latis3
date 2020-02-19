package latis.data

import scala.collection.Searching.Found

import latis.util.LatisException

/**
 * Manages a two-dimensional SampledFunction as sequences of Data.
 * This requires IndexedSeq which provides near constant-time element
 * access and length computation. The range data could be any Data type
 * while the domain data must be Datum representing Scalar variables.
 * The smart constructors ensure that sequences have consistent lengths
 * and types.
 */
class CartesianFunction2D(xs: Seq[Datum], ys: Seq[Datum], vs: Seq[Seq[RangeData]]) extends CartesianFunction {

  // Validate sizes
  //TODO enforce cartesian, move to smart const
  //{
  //  val nx = xs.length
  //  val ny = ys.length
  //  val n1 = vs.length
  //  val n2 = vs.head.length
  //  if (nx != n1 || ny != n2) {
  //    val msg = s"Domain and Range sizes don't match: ($nx, $ny), ($n1, $n2)"
  //    throw LatisException(msg)
  //  }
  //}

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
  def sampleSeq: Seq[Sample] = {
    for {
      ia <- xs.indices
      ib <- ys.indices
    } yield Sample(DomainData(xs(ia), ys(ib)), RangeData(vs(ia)(ib)))
  }
}

//TODO: fromData, fromValues, fromSeq

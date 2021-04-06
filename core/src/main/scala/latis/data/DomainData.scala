package latis.data

import cats.syntax.all._
import latis.util.LatisException

/**
 * Convenient construction and extraction methods for the
 * DomainData type alias for List[Any].
 */
object DomainData {
  /**
   * Define function to compute the distance between two
   * points defined as DomainData.
   */
  //TODO: distanceFrom method on DomainData? but juat a type alias
  val distance: (DomainData, DomainData) => Double =
    (dd1: DomainData, dd2: DomainData) => {
      //TODO: assert same length
      val squares = dd1.zip(dd2).map {
        case (Number(d1), Number(d2)) => Math.pow((d2 - d1), 2)
        case _                        => ??? //TODO: invalid data type
      }
      Math.sqrt(squares.sum)
    }

  /**
   * Construct an empty DomainData.
   */
  def apply(): DomainData = List()

  /**
   * Construct DomainData from a Seq of values.
   */
  def apply(data: Seq[Datum]): DomainData = data.toList

  /**
   * Construct DomainData from a comma separated list of values.
   */
  def apply(d: Datum, ds: Datum*): DomainData = d +: ds.toList

  /**
   * Tries to construct a DomainData from a sequence of Data.
   * This will fail if any of the elements are a SampledFunction.
   */
  def fromData(data: Seq[Data]): Either[LatisException, DomainData] =
    Data.flatten(data).traverse {
      case d: Datum => Right(d)
      case _ =>
        val msg = "DomainData cannot include SampledFunctions."
        Left(LatisException(msg))
    }

  /**
   * Tries to construct a DomainData from a varargs of Data.
   * This will fail if any of the elements are a SampledFunction.
   */
  def fromData(data: Data, datas: Data*): Either[LatisException, DomainData] =
    fromData(data +: datas)

  /**
   * Extract a comma separated list of values from DomainData.
   */
  def unapplySeq(d: DomainData): Option[Seq[Datum]] = Option(d)
}

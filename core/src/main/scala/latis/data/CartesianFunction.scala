package latis.data

import cats.implicits._
import scala.collection.Searching._
import scala.util.Try

import latis.util.CartesianDomainOrdering
import latis.util.DefaultDatumOrdering
import latis.util.LatisException
import latis.util.LatisOrdering

/**
 * Defines a base trait for a SampledFunction whose domain set can be
 * defined as a Cartesian product of independent one-dimensional domain
 * variables for each dimension.
 * This takes advantage of separable domain variables to perform binary search
 * for each dimension of the product set independently.
 */
trait CartesianFunction extends MemoizedFunction {

  /**
   * Provides a partial ordering for the domain of this CartesianFunction.
   */
  val ordering: PartialOrdering[DomainData]

  /**
   * Extracts the partial Datum ordering for the given component
   * of the Cartesian domain.
   */
  private def datumOrd(i: Int): PartialOrdering[Datum] = ordering match {
    case CartesianDomainOrdering(ords) => ords(i)
    case _ => DefaultDatumOrdering
  }

  /**
   * Performs a binary search over the given component of the Cartesian domain.
   * Searching requires total Ordering so this makes the ordering total by
   * allowing Exceptions but capturing them in the response.
   */
  private[data] def searchDomain(domainComponent: Int, values: Seq[Datum], data: Datum): Either[LatisException, SearchResult] = {
    //TODO: test if can be compared, e.g. number vs text
    //Note, IndexOutOfBounds should be safe since this is package private.
    val ord = LatisOrdering.partialToTotal(datumOrd(domainComponent))
    Try { values.search(data)(ord) }.toEither.leftMap(LatisException(_))
  }

}

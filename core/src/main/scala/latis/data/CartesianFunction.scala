package latis.data

import scala.collection.Searching._

import latis.util.LatisOrdering

/**
 * Provides a base trait for a SampledFunction whose domain set can be
 * defined as a Cartesian product of independent one-dimensional domain
 * variables for each dimension.
 * This takes advantage of separable domain variables to perform binary search
 * for each dimension of the product set independently.
 */
trait CartesianFunction extends MemoizedFunction {

  //TODO: need to get ordering from model
  val ordering: Ordering[Datum] = LatisOrdering.dataOrdering

  def searchDomain(values: Seq[Datum], data: Datum): SearchResult = {
    values.search(data)(ordering)
  }

}

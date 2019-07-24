package latis.resample

import latis.data._

/**
 * Extrapolation strategy that simply refuses by always returning None.
 */
case class NoExtrapolation() extends Extrapolation {
  
  def extrapolate(domainData: DomainData): Option[Sample] = None

}

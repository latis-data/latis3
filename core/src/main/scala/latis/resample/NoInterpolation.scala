package latis.resample

import latis.data._

/**
 * Interpolation strategy that simply refuses by always returning None.
 */
case class NoInterpolation() extends Interpolation {
  
  def interpolate(domainData: DomainData): Option[Sample] = None

}

package latis.resample

import latis.data._

/**
 * Define a trait that will resample a given SampledFunction
 * onto the given DomainSet.
 */
trait Resampling {
  
  def resample(sf: SampledFunction, domainSet: DomainSet): SampledFunction
}

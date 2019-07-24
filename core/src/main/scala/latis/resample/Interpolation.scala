package latis.resample

import latis.data._

trait Interpolation {
  //TODO: specify number of samples per dimension needed, assume 2 for now
  
  def interpolate(domainData: DomainData): Option[Sample]
}
package latis.resample

import latis.data._

trait Extrapolation {
  //TODO: specify number of samples per dimension needed, assume 1 for now
  
  def extrapolate(domainData: DomainData): Option[Sample]
}
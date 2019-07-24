package latis.resample

import latis.data._

case class NearestNeighbor(samples: Seq[Sample]) extends Interpolation with Extrapolation {
  
  def findNearest(domainData: DomainData): Option[Sample] = {
    if (samples.isEmpty) None
    else {
      val sample = samples minBy {
        case Sample(dd, _) => DomainData.distance(domainData, dd)
      }
      Some(sample)  
    }
  }
  
  def interpolate(domainData: DomainData): Option[Sample] = findNearest(domainData)
  
  def extrapolate(domainData: DomainData): Option[Sample] = findNearest(domainData)
}
package latis.ops

import latis.data._

/**
 * Reduce the samples of a SampledFunction to the one closest
 * to the domain value given to the aggregator.
 */
case class NearestNeighborAggregation() extends Aggregation {
  //Note, model shouldn't change

  def makeAggregator(domain: DomainData): Aggregator = new Aggregator {

    private var currentSample: Option[Sample] = None

    //TODO: test if we keep first nearest
    def +=(sample: Sample): this.type = {
      // Keep the sample that is closer to our domain value
      currentSample = currentSample.map { s =>
        Seq(s, sample).minBy {
          case Sample(dd, _) => DomainData.distance(domain, dd)
        }
      }.orElse {
        Some(sample) //currentSample was empty so keep this one
      }
      this
    }

    def result(): List[Data] = currentSample.map(_.range).getOrElse(List.empty)
  }
}

package latis.input

import latis.data._

import fs2._
import java.net.URI
import cats.effect.IO

/**
 * Prototype for making mock data for testing.
 */
class MockAdapter extends Adapter {
  //TODO: make data for a given model
  
  private def makeStream: Stream[IO, Sample] = {
    //TODO: use canonical test dataset
    val ss = Seq(
      Sample(DomainData(0), RangeData(0)),
      Sample(DomainData(1), RangeData(2)),
      Sample(DomainData(2), RangeData(4))
    )
    
    Stream.emits(ss)
  }
  
  def apply(uri: URI): SampledFunction = StreamFunction(makeStream)
}
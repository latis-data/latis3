package latis.input

import fs2._
import java.net.URI
import cats.effect.IO

import latis.data._
import latis.ops.Operation

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
  
  def getData(uri: URI, ops: Seq[Operation] = Seq.empty): SampledFunction =
    StreamFunction(makeStream)
}
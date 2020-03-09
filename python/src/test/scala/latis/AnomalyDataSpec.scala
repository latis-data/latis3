package latis

import latis.data._
import latis.dataset.Dataset
import latis.ops._
import latis.output.TextWriter
import latis.util.StreamUtils
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class AnomalyDataSpec extends FlatSpec {

  "The anomalous sine wave dataset" should "have the expected first sample" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies")
      .withOperation(Selection("time", ">=" , "2000-01-01"))
    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(t)),RangeData(Real(f))) =>
        t should be (1)
        f should be (0.841470985)
    }
  }

  "The anomalous sine wave dataset" should "be manipulated by a Python script" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies")
      .withOperations(Seq(Selection("time", ">=" , "2000-01-01"), 
        DetectAnomaliesWithRollingMean(10, "SineWave", "Flux")))
    
    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      //TODO: Need a Data trait to match on Booleans...
      case Sample(DomainData(Number(t)), RangeData(Real(f), Real(rm), _)) =>
        t should be (1)
        f should be (0.841470985)
        rm should be (0.9432600027000001)
        //TODO: o should be (false)
    }
    
  }
}

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

  "The rolling mean script" should "manipulate the anomalous sine wave dataset" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies")
      .withOperations(Seq(Selection("time", ">=" , "2000-01-01"),
        DetectAnomaliesWithRollingMean(dsName="SineWave")))

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(t)), RangeData(Real(f), Real(rm), BooleanDatum(o))) =>
        t should be (1)
        f should be (0.841470985)
        rm should be (0.9432600027000001)
        o should be (false)
    }
  }

  "The rolling mean script" should "also work with Text times" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies_text")
      .withOperations(Seq(Selection("time", ">=" , "2000-01-01"),
        DetectAnomaliesWithRollingMean(dsName="SineWave")))

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Text(t)), RangeData(Real(f), Real(rm), BooleanDatum(o))) =>
        t should be ("2000-01-02")
        f should be (0.841470985)
        rm should be (0.9432600027000001)
        o should be (false)
    }
  }

  "The autoencoder script" should "manipulate the anomalous sine wave dataset" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies")
      .withOperations(Seq(Selection("time", ">=" , "2000-01-01"),
        DetectAnomaliesWithAutoencoder(0.66, dsName="SineWave")))

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(t)), RangeData(Real(f), Real(a), BooleanDatum(o))) =>
        t should be (1)
        f should be (0.841470985)
        a should be (0.5363466169117648)
        o should be (false)
    }
  }

  "The autoencoder script" should "also work with Text times" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies_text")
      .withOperations(Seq(Selection("time", ">=" , "2000-01-01"),
        DetectAnomaliesWithAutoencoder(0.66, dsName="SineWave")))

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Text(t)), RangeData(Real(f), Real(a), BooleanDatum(o))) =>
        t should be ("2000-01-02")
        f should be (0.841470985)
        a should be (0.5363466169117648)
        o should be (false)
    }
  }

  "The rolling mean modeling operation" should "add its new column to the dataset." in {
    val ds = Dataset.fromName("sine_wave_with_anomalies")
      .withOperations(Seq(Selection("time", ">=" , "2000-01-01"),
        ModelWithRollingMean()))

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(t)), RangeData(Real(f), Real(rm))) =>
        t should be (1)
        f should be (0.841470985)
        rm should be (0.9432600027000001)
    }
  }
  
}

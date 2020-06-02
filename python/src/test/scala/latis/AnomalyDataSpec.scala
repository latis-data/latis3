package latis

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data._
import latis.dataset.Dataset
import latis.ops._
import latis.ops.anomalies._
import latis.output.TextWriter
import latis.util.StreamUtils

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
  
  "The rolling mean modeling operation" should "add its new column to the dataset" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies")
      .withOperation(ModelWithRollingMean())

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(t)), RangeData(Real(f), Real(rm))) =>
        t should be (1)
        f should be (0.841470985)
        rm should be (0.9432600027000001)
    }
  }

  it should "work with Text times" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies_text")
      .withOperation(ModelWithRollingMean())

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Text(t)), RangeData(Real(f), Real(rm))) =>
        t should be ("2000-01-02")
        f should be (0.841470985)
        rm should be (0.9432600027000001)
    }
  }

  "The ARIMA modeling operation" should "add its new column to the dataset" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies")
      .withOperation(ModelWithArima(0.5, (1,0,0), (0,1,0,50)))

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(t)), RangeData(Real(f), Real(a))) =>
        t should be (1)
        f should be (0.841470985)
        a.isNaN should be (true)
    }
  }

  it should "work with Text times" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies_text")
      .withOperation(ModelWithArima(0.5, (1,0,0), (0,1,0,50)))

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Text(t)), RangeData(Real(f), Real(a))) =>
        t should be ("2000-01-02")
        f should be (0.841470985)
        a.isNaN should be (true)
    }
  }

  "The robust random cut forest operation" should "add its new column to the dataset" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies")
      .withOperation(ScoreWithRrcf())

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(t)), RangeData(Real(f), Real(rrcf))) =>
        t should be (1)
        f should be (0.841470985)
        rrcf should be (0.0)
    }
  }

  it should "work with Text times" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies_text")
      .withOperation(ScoreWithRrcf())

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Text(t)), RangeData(Real(f), Real(rrcf))) =>
        t should be ("2000-01-02")
        f should be (0.841470985)
        rrcf should be (0.0)
    }
  }
  
  "The autoencoder modeling operation" should "add its new column to the dataset" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies")
      .withOperation(ModelWithAutoencoder())

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(t)), RangeData(Real(f), Real(a))) =>
        t should be (1)
        f should be (0.841470985)
        a should be (0.16296098992018704)
    }
  }

  it should "work with Text times" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies_text")
      .withOperation(ModelWithAutoencoder())

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Text(t)), RangeData(Real(f), Real(a))) =>
        t should be ("2000-01-02")
        f should be (0.841470985)
        a should be (0.16296098992018704)
    }
  }

  "The DetectAnomalies operation" should "work with a rolling mean model" in {
    val ds = Dataset.fromName("sine_wave_with_anomalies")
      .withOperations(Seq(
        ModelWithRollingMean(),
        DetectAnomalies("flux", "rollingMean", Errors, 1.0)))

    //TextWriter().write(ds)

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(t)), RangeData(Real(f), Real(rm), BooleanDatum(o))) =>
        t should be (1)
        f should be (0.841470985)
        rm should be (0.9432600027000001)
        o should be (false)
    }
  }
  
}

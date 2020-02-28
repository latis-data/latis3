package latis

import java.io.File

import latis.metadata.Metadata
import latis.util.NetUtils.resolveUri
import latis.model._
import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths

import jep.{Jep, MainInterpreter}
import latis.data.{DomainData, Integer, Number, RangeData, Real, Sample, Text}
import latis.dataset.{AdaptedDataset, Dataset}
import latis.ops.Selection
import latis.ops.DetectAnomaliesWithRollingMean
import latis.output.CsvEncoder
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
        DetectAnomaliesWithRollingMean()))

    // write a temporary CSV file of the dataset
    val csv = {
      val csvList = new CsvEncoder().encode(ds).compile.toList.unsafeRunSync()
      csvList.mkString.getBytes
    }
    //val filePath = "python/src/test/resources/data/testOutput.csv"
    val filePath = "save/ds.csv"
    Files.write(Paths.get(filePath), csv)
    
    MainInterpreter.setJepLibraryPath("/anaconda3/lib/python3.6/site-packages/jep/jep.cpython-36m-darwin.so")
    
    val dataset = filePath
    val ds_name = "SineWave"
    val var_name = "Flux"
    val alg_name = "Rolling Mean"
    val save_path = s"./save/ds_with_rolling_mean_anomalies.csv"
    val jep = new Jep()
    //jep.runScript("python/src/main/python/model_with_autoencoder.py")
    jep.runScript("python/src/main/python/detect_anomalies.py")
    //jep.runScript("python/src/main/python/nonparametric_dynamic_thresholding.py")
    //jep.runScript("python/src/main/python/model_with_arima.py")
    //jep.runScript("python/src/main/python/grid_search_hyperparameters.py")
    //jep.runScript("python/src/main/python/score_with_rrcf.py")
    jep.runScript("python/src/main/python/model_with_rolling_mean.py")
    //jep.eval(s"ts_with_model = autoencoder_prediction('$dataset', '$ds_name', var_name='$var_name', train_size=0.5)")
    //jep.eval(s"ts_with_model = model_with_arima('$dataset',0.5,(1,0,0))")
    //jep.eval(s"ts_with_model = score_with_rrcf('$dataset', '$ds_name', '$var_name')")
    jep.eval(s"ts_with_model = model_with_rolling_mean('$dataset', 10, '$ds_name', var_name='$var_name')")
    jep.eval(s"X = ts_with_model['$var_name']")
    jep.eval(s"Y = ts_with_model['$alg_name']")
    jep.eval(s"ts_with_anomalies = detect_anomalies(X, Y, '$ds_name', '$var_name', '$alg_name', outlier_def='errors', num_stds=2, data_save_path='$save_path')")
   

    // delete the temporary file
    //new File(filePath).delete()
  }
}

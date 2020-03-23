package latis.ops

import jep.{MainInterpreter, NDArray, SharedInterpreter}
import latis.data._
import latis.metadata.Metadata
import latis.model._

/**
 * Defines an Operation that detects anomalies in a univariate time series
 * (i.e., time -> value) with an autoencoder algorithm.
 *
 * @param trainSize the percentage of data to use for training
 * @param outlierDef the definition of an outlier to be used—either "errors," "std," or "dynamic"
 * @param sigma the number of standard deviations away from the mean used to define point outliers
 * @param dsName the name of the dataset
 */
case class DetectAnomaliesWithAutoencoder(
  trainSize: Double = 1.0,
  outlierDef: String = "errors",
  sigma: Int = 2,
  dsName: String = "Dataset") extends UnaryOperation {

  /**
   * Adds two new variables to the model's range,
   * "autoencoder" and "outlier," such that the model 
   * becomes (time -> (value, autoencoder, outlier).
   */
  override def applyToModel(model: DataType): DataType = {
    model match {
      case Function(d, r: Scalar) => 
        val rm = Scalar(
          Metadata(
            "id" -> "autoencoder",
            "type" -> "double"
          )
        )
        val o = Scalar(
          Metadata(
            "id" -> "outlier",
            "type" -> "boolean"
          )
        )
        Function(d, Tuple(r, rm, o))
      case _ => ??? //invalid
    }
  }

  /**
   * Detects anomalies in the time series with two Python scripts.
   * The first models the time series with an autoencoder (using the 
   * specified training size), and the second compares the time series 
   * with the autoencoder's predictions to label anomalies based on the 
   * specified outlier definition.
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    val samples = data.unsafeForce.sampleSeq
    val samplesForPython: Array[Array[Double]] = samples.map {
      case Sample(DomainData(Index(i)), RangeData(Real(f))) => Array(i, f)
    }.toArray

    MainInterpreter.setJepLibraryPath("/anaconda3/lib/python3.7/site-packages/jep/jep.cpython-37m-darwin.so")
    val interp = new SharedInterpreter
    
    try {
      interp.runScript("python/src/main/python/model_with_autoencoder.py")
      interp.runScript("python/src/main/python/detect_anomalies.py")
      
      val ds_name = dsName
      val var_name = model match {
        case Function(_, r: Scalar) =>
          r.id        
      }
      val alg_name = "Autoencoder"
      //val save_path = "./save/ds_with_autoencoder_anomalies.csv"
      //val plot_path = "./save/ds_with_autoencoder_anomalies.png"
      
      //TODO: consider building "samplesForPython" as it's being passed in to avoid storing it for longer than needed
      val ds_numpy: NDArray[Array[Double]] = new NDArray[Array[Double]](samplesForPython.flatten, samplesForPython.length, samplesForPython(0).length)
      interp.set("ds_numpy", ds_numpy)
      
      interp.exec(s"ts_with_model = autoencoder_prediction(ds_numpy, '$ds_name', $trainSize, var_name='$var_name')")
      interp.exec(s"X = ts_with_model['$var_name']")
      interp.exec(s"Y = ts_with_model['$alg_name']")
      interp.exec(s"ts_with_anomalies = detect_anomalies(X, Y, '$ds_name', '$var_name', '$alg_name', outlier_def='$outlierDef', num_stds=$sigma)")
      
      interp.exec(s"predictions = ts_with_anomalies.$alg_name.to_numpy()")
      interp.exec("outliers = ts_with_anomalies.Outlier.to_numpy()")
      val autoencoderCol = interp.getValue("predictions", classOf[NDArray[Array[Double]]]).getData
      val outlierCol = interp.getValue("outliers", classOf[NDArray[Array[Boolean]]]).getData
      
      //Reconstruct the SampledFunction with the new data
      //TODO: is .zipWithIndex noticeably slower than a manual for loop?
      val samplesWithAnomalyData: Seq[Sample] = samples.zipWithIndex.map { case (smp, i) =>
        smp match {
          case Sample(dd, rd) => {
             Sample(dd, rd :+ Data.DoubleValue(autoencoderCol(i)) :+ Data.BooleanValue(outlierCol(i))) 
          }
        }
      }
      
      SampledFunction(samplesWithAnomalyData)
    } finally if (interp != null) {
      interp.close()
    }
  }

}

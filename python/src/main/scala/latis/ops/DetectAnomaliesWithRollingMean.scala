package latis.ops

import jep.{Interpreter, Jep, MainInterpreter, NDArray, SharedInterpreter}
import latis.data._
import latis.model._
import latis.metadata.Metadata

/**
 * Defines an Operation that detects anomalies in a univariate time series
 * (i.e., time -> value) with a rolling mean algorithm.
 */
case class DetectAnomaliesWithRollingMean(
  window: Int = 10, 
  dsName: String = "Dataset", 
  varName: String = "Value",
  outlierDef: String = "errors",
  numStds: Int = 2) extends UnaryOperation {

  /**
   * Provides a new model resulting from this Operation.
   */
  override def applyToModel(model: DataType): DataType = {
    model match {
      case Function(d, r: Scalar) => 
        val rm = Scalar(
          Metadata(
            "id" -> "rollingMean",
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
   * Provides new Data resulting from this Operation.
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    val allData = data.unsafeForce
    val samples = allData.sampleSeq
    val samplesForPython: Array[Array[Double]] = samples.map {
      case Sample(DomainData(Index(i)), RangeData(Real(f))) => Array(i, f)
    }.toArray

    MainInterpreter.setJepLibraryPath("/anaconda3/lib/python3.6/site-packages/jep/jep.cpython-36m-darwin.so")
    
    val interp = new SharedInterpreter
    try {
      interp.runScript("python/src/main/python/model_with_rolling_mean.py")
      interp.runScript("python/src/main/python/detect_anomalies.py")
      
      val ds_name = dsName
      val var_name = varName
      val alg_name = "Rolling_Mean"
//      val save_path = "./save/ds_with_rolling_mean_anomalies.csv"
//      val plot_path = "./save/ds_with_rolling_mean_anomalies.png"
      
      //TODO: consider making a python list var [] and appending NDArrays[Array[Double]] to it in samplesForPython's map
      val ds_numpy: NDArray[Array[Double]] = new NDArray[Array[Double]](samplesForPython.flatten, samplesForPython.length, samplesForPython(0).length)
      interp.set("ds_numpy", ds_numpy)
      interp.exec(s"ts_with_model = model_with_rolling_mean(ds_numpy, $window, '$ds_name', var_name='$var_name')")
      interp.exec(s"X = ts_with_model['$var_name']")
      interp.exec(s"Y = ts_with_model['$alg_name']")
      interp.exec(s"ts_with_anomalies = detect_anomalies(X, Y, '$ds_name', '$var_name', '$alg_name', outlier_def='$outlierDef', num_stds=$numStds)")
      
      //TODO: don't hardcode all this
      interp.exec("rollingMeans = ts_with_anomalies.Rolling_Mean.to_numpy()")
      interp.exec("outliers = ts_with_anomalies.Outlier.to_numpy()")
      val rollingMeanCol = interp.getValue("rollingMeans", classOf[NDArray[Array[Double]]]).getData
      val outlierCol = interp.getValue("outliers", classOf[NDArray[Array[Boolean]]]).getData
      
      //Reconstruct the SampledFunction with the new data
      //TODO: is .zipWithIndex noticeably slower than a manual for loop?
      val samplesWithAnomalyData: Seq[Sample] = samples.zipWithIndex.map { case (smp, i) =>
        smp match {
          case Sample(dd, rd) => {
             Sample(dd, rd :+ Data.DoubleValue(rollingMeanCol(i)) :+ Data.BooleanValue(outlierCol(i))) 
          }
        }
      }
      
      SampledFunction(samplesWithAnomalyData)
    } finally if (interp != null) {
      interp.close()
    }
  }

}

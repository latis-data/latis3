package latis.ops

import jep.{Interpreter, Jep, JepException, MainInterpreter, NDArray, SharedInterpreter}
import latis.data._
import latis.model._
import latis.metadata.Metadata
import latis.time.{Time, TimeFormat, TimeScale}
import latis.units.UnitConverter

/**
 * Defines an Operation that detects anomalies in a univariate time series
 * (i.e., time -> value) with a rolling mean algorithm.
 * 
 * @param window the window size used for the rolling mean
 * @param outlierDef the definition of an outlier to be used—either "errors," "std," or "dynamic"
 * @param sigma the number of standard deviations away from the mean used to define point outliers
 * @param dsName the name of the dataset
 */
case class DetectAnomaliesWithRollingMean(
  window: Int = 10,
  outlierDef: String = "errors",
  sigma: Int = 2,
  dsName: String = "Dataset") extends UnaryOperation {

  /**
   * Adds two new variables to the model's range,
   * "rollingMean" and "outlier," such that the model 
   * becomes (time -> (value, rollingMean, outlier).
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
   * Detects anomalies in the time series with two Python scripts.
   * The first models the time series with a rolling mean (using the 
   * specified window size), and the second compares the time series 
   * with the rolling mean to label anomalies based on the specified
   * outlier definition.
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    val samples = data.unsafeForce.sampleSeq
    
    val samplesForPython: Array[Array[Double]] = model match {
      case Function(time: Time, _: Scalar) =>
        //convert all Time values to ms since 1970-01-01 for the Python scripts
        if (time.isFormatted) { //textual Time
          val tf = time.timeFormat.get
          samples.map {
            case Sample(DomainData(Text(t)), RangeData(Number(n))) =>
              Array(tf.parse(t).asInstanceOf[Double], n) //TODO: properly cast to Double
          }.toArray
        } else { //numeric Time
          val uc = UnitConverter(time.timeScale, TimeScale.Default)
          samples.map {
            case Sample(DomainData(Number(t)), RangeData(Number(n))) =>
              Array(uc.convert(t).asInstanceOf[Double], n) //TODO: properly cast to Double
          }.toArray
        }
    }
    
    try {
      MainInterpreter.setJepLibraryPath(System.getProperty("user.dir") + "/python/lib/jep.cpython-36m-darwin.so")
    } catch {
      case _: JepException => //JEP library path already set
    }
    
    val interp = new SharedInterpreter
    
    try {
      interp.runScript("python/src/main/python/model_with_rolling_mean.py")
      interp.runScript("python/src/main/python/detect_anomalies.py")
      
      val ds_name = dsName
      val var_name = model match {
        case Function(_, r: Scalar) =>
          r.id        
      }
      val alg_name = "Rolling_Mean"
      //val save_path = "./save/ds_with_rolling_mean_anomalies.csv"
      //val plot_path = "./save/ds_with_rolling_mean_anomalies.png"
      
      //TODO: consider building "samplesForPython" as it's being passed in to avoid storing it for longer than needed
      val ds_numpy: NDArray[Array[Double]] = new NDArray[Array[Double]](samplesForPython.flatten, samplesForPython.length, samplesForPython(0).length)
      interp.set("ds_numpy", ds_numpy)
      
      interp.exec(s"ts_with_model = model_with_rolling_mean(ds_numpy, $window, '$ds_name', var_name='$var_name')")
      interp.exec(s"X = ts_with_model['$var_name']")
      interp.exec(s"Y = ts_with_model['$alg_name']")
      interp.exec(s"ts_with_anomalies = detect_anomalies(X, Y, '$ds_name', '$var_name', '$alg_name', outlier_def='$outlierDef', num_stds=$sigma)")
      
      interp.exec(s"rollingMeans = ts_with_anomalies.$alg_name.to_numpy()")
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

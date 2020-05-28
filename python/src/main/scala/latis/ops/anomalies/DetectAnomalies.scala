package latis.ops.anomalies

import jep.NDArray
import jep.SharedInterpreter

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.ops.JepOperation
import latis.time.Time
import latis.time.TimeScale
import latis.units.UnitConverter
import latis.util.LatisException

/**
 * Defines an Operation that detects anomalies in a univariate time series
 * by comparing the original values (`X`) with modeled values (`Y`), using a
 * specified anomaly definition (`anomalyDef` and `sigma`). A new boolean 
 * variable is added to the Dataset to label whether each point is anomalous.
 *
 * @param X the name of the variable that stores the original time series values
 * @param Y the name of the variable that stores the modeled time series values
 * @param anomalyDef the definition of an anomaly to be used—either Errors, Std, or Dynamic
 * @param sigma the number of standard deviations away from the mean used to define point anomalies
 */
case class DetectAnomalies(
  X: String,
  Y: String,
  anomalyDef: AnomalyDef = Errors,
  sigma: Double = 2.0) extends JepOperation {

  /** The name of the interpreter variable that stores the time series. */
  protected val interpDs = "dataset"

  /**
   * Adds a new "anomaly" variable to the model's range.
   */
  override def applyToModel(model: DataType): DataType = {
    model match {
      case Function(d, r: Tuple) =>
        val a = Scalar(
          Metadata(
            "id" -> "anomaly",
            "type" -> "boolean"
          )
        )
        //TODO: is keeping metadata valid since we're adding an element?
        Function(d, Tuple(r.metadata, r.elements :+ a))
      case _ => ??? //invalid
    }
  }

  /**
   * Detects anomalies in the time series with Python code that compares the original
   * values (`X`) with modeled values (`Y`) based on the specified anomaly definition.
   * Adds the "anomaly" variable to the SampledFunction's range.
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    setJepPath
    val interp = new SharedInterpreter
    val samples = data.unsafeForce.sampleSeq

    try {
      interp.runScript("python/src/main/python/detect_anomalies.py")

      copyDataForPython(interp, model, samples)
      val anomalyCol = runAnomalyDetectionCode(interp)

      //Reconstruct the SampledFunction with the anomaly data included
      //TODO: is .zipWithIndex noticeably slower than a manual for loop?
      val samplesWithAnomalyData: Seq[Sample] = samples.zipWithIndex.map { case (smp, i) =>
        smp match {
          case Sample(dd, rd) => {
            Sample(dd, rd :+ Data.BooleanValue(anomalyCol(i)))
          }
        }
      }
      SampledFunction(samplesWithAnomalyData)
    } finally if (interp != null) {
      interp.close()
    }
  }

  /**
   * Copies into the interpreter the Time, X, and Y data with time values formatted as ms since 1970-01-01.
   */
  private def copyDataForPython(interpreter: SharedInterpreter, model: DataType, samples: Seq[Sample]): Unit = {
    val xPos = model.getPath(X) match {
      case Some(List(RangePosition(n))) => n
      case _ => throw new LatisException(s"Cannot find variable: $X")
    }
    val yPos = model.getPath(Y) match {
      case Some(List(RangePosition(n))) => n
      case _ => throw new LatisException(s"Cannot find variable: $Y")
    }
    
    interpreter.set(interpDs, new NDArray[Array[Double]](
      model match {
        case Function(time: Time, _) =>
          if (time.isFormatted) { //text times
            val tf = time.timeFormat.get
            samples.map {
              case Sample(DomainData(Text(t)), r: RangeData) =>
                tf.parse(t) match {
                  case Right(v) =>
                    val x = r(xPos) match { case Number(n) => n }
                    val y = r(yPos) match { case Number(n) => n }
                    Array(v, x, y)
                }
            }.toArray.flatten
          } else { //numeric times
            val uc = UnitConverter(time.timeScale, TimeScale.Default)
            samples.map {
              case Sample(DomainData(Number(t)), r: RangeData) =>
                val x = r(xPos) match { case Number(n) => n }
                val y = r(yPos) match { case Number(n) => n }
                Array(uc.convert(t), x, y)
            }.toArray.flatten
          }
      }, samples.length, 3)
    )
  }

  /**
   * Executes Python code that stores the data in pandas Series before passing
   * them to the anomaly detection script. Returns the new "anomaly" column.
   */
  private def runAnomalyDetectionCode(interpreter: SharedInterpreter): Array[Boolean] = {
    interpreter.exec("import pandas as pd")
    interpreter.exec(s"ts = pd.DataFrame(data=$interpDs, columns=['Time', '$X', '$Y'])")
    interpreter.exec("ts['Time'] = ts['Time'].apply(lambda t: pd.to_datetime(t, unit='ms', origin='unix'))")
    interpreter.exec("ts = ts.set_index('Time')")
    interpreter.exec(s"X = ts['$X']")
    interpreter.exec(s"Y = ts['$Y']")
    interpreter.exec(s"ts_with_anomalies = detect_anomalies(X, Y, outlier_def='${anomalyDef.asString}', num_stds=$sigma)")
    interpreter.exec("outliers = ts_with_anomalies.Outlier.to_numpy()")
    interpreter.getValue("outliers", classOf[NDArray[Array[Boolean]]]).getData
  }
  
}

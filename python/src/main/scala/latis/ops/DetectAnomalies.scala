package latis.ops

import jep.JepException
import jep.MainInterpreter
import jep.NDArray
import jep.SharedInterpreter
import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.time.TimeScale
import latis.units.UnitConverter

/**
 * Defines an Operation that detects anomalies in a univariate time series
 * by comparing the original values (`X`) with modeled values (`Y`), using a
 * specified anomaly definition (`anomalyDef` and `sigma`). A new boolean 
 * variable is added to the Dataset to label whether each point is anomalous.
 *
 * @param X the name of the variable that stores the original time series values
 * @param Y the name of the variable that stores the modeled time series values
 * @param anomalyDef the definition of an anomaly to be used—either "errors", "std", or "dynamic"
 * @param sigma the number of standard deviations away from the mean used to define point anomalies
 */
case class DetectAnomalies(
  X: String,
  Y: String,
  anomalyDef: String = "errors",
  sigma: Int = 2) extends UnaryOperation {

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
   * Detects anomalies in the time series with a Python script that compares the
   * original values (`X`) with modeled values (`Y`) based on the specified anomaly
   * definition. Adds the "anomaly" variable to the SampledFunction's range.
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    setJepPath
    val interp = new SharedInterpreter
    val samples = data.unsafeForce.sampleSeq

    try {
      interp.runScript("python/src/main/python/detect_anomalies.py")

      interp.set("dataset", new NDArray[Array[Double]](
        //copy the Time, X, and Y data with time values formatted as ms since 1970-01-01
        model match {
          case Function(time: Time, _) =>
            if (time.isFormatted) { //text times
              val tf = time.timeFormat.get
              samples.map {
                case Sample(DomainData(Text(t)), r: RangeData) =>
                  tf.parse(t) match {
                    case Right(v) => 
                      ???
                      //TODO: find X and Y in r by name, then Array(v, X, Y)
                  }
              }.toArray.flatten
            } else { //numeric times
              val uc = UnitConverter(time.timeScale, TimeScale.Default)
              samples.map {
                case Sample(DomainData(Number(t)), r: RangeData) =>
                  ???
                  //TODO: find X and Y in r by name, then Array(uc.convert(t), X, Y)
              }.toArray.flatten
            }
        }, samples.length, 3) //TODO: is this shape right?
      )

      //TODO: turn "dataset" into a two pandas Series, X and Y
      //TODO: populate ts_with_anomalies
      interp.exec(s"ts_with_anomalies = detect_anomalies(X, Y, outlier_def='$anomalyDef', num_stds=$sigma)")
      interp.exec("outliers = ts_with_anomalies.Outlier.to_numpy()")
      val anomalyCol = interp.getValue("outliers", classOf[NDArray[Array[Boolean]]]).getData

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
   * Set the path to the JEP library file if it hasn't already been set.
   */
  private def setJepPath: Unit = try {
    MainInterpreter.setJepLibraryPath(System.getProperty("user.dir") + "/python/lib/jep.cpython-36m-darwin.so")
  } catch {
    case _: JepException => //JEP library path already set
  }
  
}

package latis.ops

import jep.{JepException, MainInterpreter, NDArray, SharedInterpreter}
import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.{Time, TimeScale}
import latis.units.UnitConverter

/**
 * Defines an Operation that detects anomalies in a univariate time series
 * by comparing the original values (`X`) with modeled values (`Y`), using a
 * specified anomaly definition (`outlierDef` and `sigma`). A new boolean 
 * variable is added to the Dataset to label whether each point is anomalous.
 *
 * @param X the name of the variable that stores the original time series values
 * @param Y the name of the variable that stores the modeled time series values
 * @param outlierDef the definition of an outlier to be used—either "errors", "std", or "dynamic"
 * @param sigma the number of standard deviations away from the mean used to define point outliers
 * @param dsName the name of the dataset
 * //TODO: standardize language on either "anomaly" or "outlier" (probably "anomaly")?
 */
case class DetectAnomalies(
  X: String,
  Y: String,
  outlierDef: String = "errors",
  sigma: Int = 2,
  dsName: String = "Dataset") extends UnaryOperation {

  /**
   * Adds a new "outlier" variable to the model's range.
   */
  override def applyToModel(model: DataType): DataType = {
    model match {
      case Function(d, r: Tuple) =>
        val o = Scalar(
          Metadata(
            "id" -> "outlier",
            "type" -> "boolean"
          )
        )
        //TODO: is keeping metadata valid?
        Function(d, Tuple(r.metadata, r.elements :+ o))
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
    setJepPath
    val interp = new SharedInterpreter

    val samples = data.unsafeForce.sampleSeq
    
    //Store X and Y along with the Time index
//    val pythonData: Array[(String, Double, Double)] = samples.map {
//      case Sample(, RangeData(_)) => ???
//    }

    interp.set("dataset", new NDArray[Array[Double]](
      //copy all the data with time values formatted as ms since 1970-01-01
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
      }, samples.length, 2)
    )
    
    //TODO: turn "dataset" into a pandas Dataframe
    
    //data
//    setJepPath
//    val interp = new SharedInterpreter
//    val algName = "Rolling_Mean"
//    val varName = model match {
//      case Function(_, r: Scalar) => r.id
//    }
//    val samples = data.unsafeForce.sampleSeq
//    
//    try {
//      interp.runScript("python/src/main/python/model_with_rolling_mean.py")
//      interp.runScript("python/src/main/python/detect_anomalies.py")
//      
//      interp.set("dataset", new NDArray[Array[Double]](
//        //copy all the data with time values formatted as ms since 1970-01-01
//        model match {
//          case Function(time: Time, _) =>
//            if (time.isFormatted) { //text times
//              val tf = time.timeFormat.get
//              samples.map {
//                case Sample(DomainData(Text(t)), RangeData(Number(n))) =>
//                  tf.parse(t) match {
//                    case Right(v) => Array(v, n)
//                  }
//              }.toArray.flatten
//            } else { //numeric times
//              val uc = UnitConverter(time.timeScale, TimeScale.Default)
//              samples.map {
//                case Sample(DomainData(Number(t)), RangeData(Number(n))) =>
//                  Array(uc.convert(t), n)
//              }.toArray.flatten
//            }
//        }, samples.length, 2)
//      )
//
//      interp.exec(s"ts_with_model = model_with_rolling_mean(dataset, $window, '$algName', '$varName')")
//      interp.exec(s"X = ts_with_model['$varName']")
//      interp.exec(s"Y = ts_with_model['$algName']")
//      interp.exec(s"ts_with_anomalies = detect_anomalies(X, Y, '$dsName', '$varName', '$algName', outlier_def='$outlierDef', num_stds=$sigma)")
//      
//      interp.exec(s"modelOutput = ts_with_anomalies.$algName.to_numpy()")
//      interp.exec("outliers = ts_with_anomalies.Outlier.to_numpy()")
//      
//      val modelOutputCol = interp.getValue("modelOutput", classOf[NDArray[Array[Double]]]).getData
//      val outlierCol = interp.getValue("outliers", classOf[NDArray[Array[Boolean]]]).getData
//
//      //Reconstruct the SampledFunction with the anomaly data included
//      //TODO: is .zipWithIndex noticeably slower than a manual for loop?
//      val samplesWithAnomalyData: Seq[Sample] = samples.zipWithIndex.map { case (smp, i) =>
//        smp match {
//          case Sample(dd, rd) => {
//             Sample(dd, rd :+ Data.DoubleValue(modelOutputCol(i)) :+ Data.BooleanValue(outlierCol(i))) 
//          }
//        }
//      }
//      SampledFunction(samplesWithAnomalyData)
//    } finally if (interp != null) {
//      interp.close()
//    }
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

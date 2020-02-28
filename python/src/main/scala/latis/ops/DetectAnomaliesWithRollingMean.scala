package latis.ops

import jep.{Jep, MainInterpreter, Interpreter, SharedInterpreter, NDArray}
import latis.data.{DomainData, Index, Integer, Number, RangeData, Real, Sample, SampledFunction}
import latis.model.DataType

/**
 * Defines an Operation that acts on a single Dataset.
 */
case class DetectAnomaliesWithRollingMean() extends UnaryOperation {

  /**
   * Provides a new model resulting from this Operation.
   */
  override def applyToModel(model: DataType): DataType = {
    model
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
    //val jep = new Jep()
    try {
      //interp.exec("import model_with_rolling_mean")
      interp.runScript("python/src/main/python/model_with_rolling_mean.py")
      //jep.runScript("python/src/main/python/model_with_rolling_mean.py")
      
      // any of the following work, these are just pseudo-examples
      
      // using exec(String) to invoke methods
      val ds_name = "SineWave"
      val var_name = "Flux"
      val alg_name = "Rolling Mean"
      val save_path = "./save/ds_with_rolling_mean_anomalies.csv"
      //TODO: consider making a python list var [] and appending NDArrays[Array[Double]] to it in samplesForPython's map
      val ds_numpy: NDArray[Array[Double]] = new NDArray[Array[Double]](samplesForPython.flatten, samplesForPython.length, samplesForPython(0).length)
      interp.set("ds_numpy", ds_numpy)
      interp.exec(s"z = model_with_rolling_mean(ds_numpy, 10, '$ds_name', var_name='$var_name')")
      
      val result = interp.getValue("z")
      println(result)
      
    } finally if (interp != null) {
      interp.close()
      //jep.close()
    }
    
//    try {
//      val interp = new SharedInterpreter
//      try {
//        val f = Array[Float](1.0f, 2.1f, 3.3f, 4.5f, 5.6f, 6.7f)
//        val nd = new NDArray[Array[Float]](f, 3, 2)
//        interp.set("x", nd)
//      } finally if (interp != null) interp.close()
//    }
    
//    val ds_name = "SineWave"
//    val var_name = "Flux"
//    val alg_name = "Rolling Mean"
//    val save_path = s"./save/ds_with_rolling_mean_anomalies.csv"
//    val jep = new Jep()
//    jep.runScript("python/src/main/python/detect_anomalies.py")
//    jep.runScript("python/src/main/python/model_with_rolling_mean.py")
//
//    jep.eval(s"ts_with_model = model_with_rolling_mean($samplesForPython, 10, '$ds_name', var_name='$var_name')")
//    jep.eval(s"X = ts_with_model['$var_name']")
//    jep.eval(s"Y = ts_with_model['$alg_name']")
//    jep.eval(s"ts_with_anomalies = detect_anomalies(X, Y, '$ds_name', '$var_name', '$alg_name', outlier_def='errors', num_stds=2, data_save_path='$save_path')")



    ???
  }

}

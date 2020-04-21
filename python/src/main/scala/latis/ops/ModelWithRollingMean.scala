package latis.ops

import jep.NDArray
import jep.SharedInterpreter

/**
 * Defines an Operation that models a univariate time series
 * (i.e., time -> value) with a rolling mean algorithm.
 */
case class ModelWithRollingMean(window: Int = 10) extends ModelTimeSeries {
  
  def pathToModelScript: String = "python/src/main/python/model_with_rolling_mean.py"

  def modelAlg: String = "rollingMean"
  
  def modelVarType: String = "double"

  /**
   * Executes Python code that models the dataset with a rolling mean then returns the model's output.
   */
  def runModelingAlgorithm(interpreter: SharedInterpreter): Array[Double] = {
    interpreter.exec(s"ts_with_model = model_with_rolling_mean($interpreterDsName, $window, '$modelAlg')")
    interpreter.exec(s"model_output = ts_with_model.$modelAlg.to_numpy()")
    interpreter.getValue("model_output", classOf[NDArray[Array[Double]]]).getData
  }
 
}

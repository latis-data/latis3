package latis.ops.anomalies

import jep.NDArray
import jep.SharedInterpreter

import latis.util.Identifier
import latis.util.Identifier._

/**
 * Defines an Operation that models a univariate time series
 * (i.e., time -> value) with a rolling mean algorithm.
 * 
 *  @param window the window size used for the rolling mean
 */
case class ModelWithRollingMean(window: Int = 10) extends ModelTimeSeries {
  
  def modelScript: String = "model_with_rolling_mean.py"

  def modelAlg: Identifier = id"rollingMean"
  
  def modelVarType: String = "double"

  /**
   * Executes Python code that models the dataset with a rolling mean then returns the model's output.
   */
  def runModelingAlgorithm(interpreter: SharedInterpreter): Array[Double] = {
    interpreter.exec(s"ts_with_model = model_with_rolling_mean($interpDs, $window, '${modelAlg.asString}')")
    interpreter.exec(s"model_output = ts_with_model.${modelAlg.asString}.to_numpy()")
    interpreter.getValue("model_output", classOf[NDArray[Array[Double]]]).getData
  }
 
}

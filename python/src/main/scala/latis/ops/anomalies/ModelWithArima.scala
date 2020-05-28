package latis.ops.anomalies

import jep.NDArray
import jep.SharedInterpreter

/**
 * Defines an Operation that models a univariate time series
 * (i.e., time -> value) with an ARIMA algorithm.
 * 
 * @param trainSize the percentage of data used as the training set
 * @param order the order hyperparameters (p,d,q) for the ARIMA model
 * @param seasonalOrder the seasonal order hyperparameters (P,D,Q,s) for the SARIMA model
 * @param trend the trend hyperparameter for the SARIMA model—either "n", "c", "t", or "ct"
 * @param gridSearch when true, performs a grid search to set hyperparameter values (overrides the given values)
 */
case class ModelWithArima(
  trainSize: Double = 1.0,
  order: (Int, Int, Int),
  seasonalOrder: (Int, Int, Int, Int),
  trend: String = "c",
  gridSearch: Boolean = false) extends ModelTimeSeries {
  
  def pathToModelScript: String = "python/src/main/python/model_with_arima.py"

  def modelAlg: String = "arima"
  
  def modelVarType: String = "double"

  /**
   * Executes Python code that models the dataset with an ARIMA model then returns the model's output.
   */
  def runModelingAlgorithm(interpreter: SharedInterpreter): Array[Double] = {
    interpreter.exec(s"ts_with_model = model_with_arima($interpDs," +
      s"$trainSize," +
      s"(${order._1}, ${order._2}, ${order._3})," +
      s"(${seasonalOrder._1}, ${seasonalOrder._2}, ${seasonalOrder._3})," +
      s"${seasonalOrder._4}," +
      s"'$trend'," +
      s"${if (gridSearch) "True" else "False"}," +
      s"col_name='$modelAlg')")
    interpreter.exec(s"model_output = ts_with_model.$modelAlg.to_numpy()")
    interpreter.getValue("model_output", classOf[NDArray[Array[Double]]]).getData
  }
 
}

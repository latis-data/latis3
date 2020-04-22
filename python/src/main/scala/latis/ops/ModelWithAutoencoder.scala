package latis.ops

import jep.{NDArray, SharedInterpreter}

/**
 * Defines an Operation that models a univariate time series
 * (i.e., time -> value) with a rolling mean algorithm.
 */
case class ModelWithAutoencoder(trainSize: Double = 1.0) extends ModelTimeSeries {
  
  def pathToModelScript: String = "python/src/main/python/model_with_autoencoder.py"

  def modelAlg: String = "autoencoder"
  
  def modelVarType: String = "double"

  /**
   * Executes Python code that models the dataset with a rolling mean then returns the model's output.
   */
  def runModelingAlgorithm(interpreter: SharedInterpreter): Array[Double] = {
    interpreter.exec(s"ts_with_model = autoencoder_prediction($interpDs, $trainSize, col_name='$modelAlg')")
    interpreter.exec(s"model_output = ts_with_model.$modelAlg.to_numpy()")
    interpreter.getValue("model_output", classOf[NDArray[Array[Double]]]).getData
  }
 
}

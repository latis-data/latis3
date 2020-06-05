package latis.ops.anomalies

import jep.NDArray
import jep.SharedInterpreter

import latis.util.Identifier
import latis.util.Implicits._

/**
 * Defines an Operation that models a univariate time series
 * (i.e., time -> value) with an autoencoder algorithm.
 * 
 * @param trainSize the percentage of data used as the training set
 */
case class ModelWithAutoencoder(trainSize: Double = 1.0) extends ModelTimeSeries {
  
  def modelScript: String = "model_with_autoencoder.py"

  def modelAlg: Identifier = id"autoencoder"
  
  def modelVarType: String = "double"

  /**
   * Executes Python code that models the dataset with an autoencoder then returns the model's output.
   */
  def runModelingAlgorithm(interpreter: SharedInterpreter): Array[Double] = {
    interpreter.exec(s"ts_with_model = autoencoder_prediction($interpDs, $trainSize, col_name='${modelAlg.toString}')")
    interpreter.exec(s"model_output = ts_with_model.${modelAlg.toString}.to_numpy()")
    interpreter.getValue("model_output", classOf[NDArray[Array[Double]]]).getData
  }
 
}

package latis.ops.anomalies

import jep.NDArray
import jep.SharedInterpreter

import latis.util.Identifier
import latis.util.IdentifierInterpolator.IdentifierStringContext

/**
 * Defines an Operation that assigns anomaly scores to points in a univariate 
 * time series (i.e., time -> value) with a robust random cut forest algorithm.
 * 
 * @param numTrees the number of trees in the generated forest
 * @param shingleSize the size of each shingle when shingling the time series
 * @param treeSize the size of each tree in the generated forest
 */
case class ScoreWithRrcf(
  numTrees: Int = 100,
  shingleSize: Int = 18,
  treeSize: Int = 256) extends ModelTimeSeries {
  
  def modelScript: String = "score_with_rrcf.py"

  def modelAlg: Identifier = id"rrcf"
  
  def modelVarType: String = "double"

  /**
   * Executes Python code that scores the dataset with a robust random cut forest then returns the scores.
   */
  def runModelingAlgorithm(interpreter: SharedInterpreter): Array[Double] = {
    interpreter.exec(s"ts_with_scores = score_with_rrcf($interpDs," +
      s"num_trees=$numTrees," +
      s"shingle_size=$shingleSize," +
      s"tree_size=$treeSize," +
      s"col_name='${modelAlg.toString}')")
    interpreter.exec(s"scores = ts_with_scores.${modelAlg.toString}.to_numpy()")
    interpreter.getValue("scores", classOf[NDArray[Array[Double]]]).getData
  }
 
}

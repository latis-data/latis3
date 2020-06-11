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
import latis.util.Identifier
import latis.util.LatisConfig

/**
 * Defines an Operation that models a univariate time series
 * (i.e., time -> value) with a specified algorithm.
 * TODO: Should we avoid overloading the term "model"?
 */
trait ModelTimeSeries extends JepOperation {

  /** 
   * The name of the modeling algorithm (must be a valid LaTiS identifier).
   * Becomes the name of the new Dataset variable that holds the modeled time series values.
   */
  def modelAlg: Identifier

  /** The type of the new Dataset variable. */
  def modelVarType: String

  /**
   * The name of the Python script that models the time series.
   */
  def modelScript: String

  /**
   * Executes Python code that models the dataset then returns the model's output.
   */
  def runModelingAlgorithm(interpreter: SharedInterpreter): Array[Double]
  
  /** The name of the interpreter variable that stores the time series. */
  protected val interpDs = "dataset"
  
  /**
   * Adds a new variable to the model's range named `modelAlg` 
   * (of type `modelVarType`) such that the model becomes 
   * (time -> (value, `modelAlg`).
   */
  override def applyToModel(model: DataType): DataType = {
    model match {
      case Function(d, r: Scalar) =>
        val m = Scalar(
          Metadata(
            "id" -> modelAlg.asString,
            "type" -> modelVarType
          )
        )
        Function(d, Tuple(r, m))
      case _ => ??? //invalid
    }
  }

  /**
   * Models the time series with the given Python script.
   */
  override def applyToData(data: SampledFunction, model: DataType): SampledFunction = {
    setJepPath

    val interp = new SharedInterpreter
    val pythonDir = LatisConfig.getOrElse("latis.python.path", "python/src/main/python")
    val samples = data.unsafeForce.sampleSeq

    try {
      interp.runScript(s"$pythonDir/$modelScript")
      copyDataForPython(interp, model, samples)
      val modelOutputCol = runModelingAlgorithm(interp)

      //Reconstruct the SampledFunction with the modeled data included
      val samplesWithModelData: Seq[Sample] = samples.zipWithIndex.map { case (smp, i) =>
        smp match {
          case Sample(dd, rd) => {
            Sample(dd, rd :+ Data.DoubleValue(modelOutputCol(i)))
          }
        }
      }
      SampledFunction(samplesWithModelData)
    } finally if (interp != null) {
      interp.close()
    }
  }

  /**
   * Copies into the interpreter all the data with time values formatted as ms since 1970-01-01.
   */
  private def copyDataForPython(interpreter: SharedInterpreter, model: DataType, samples: Seq[Sample]): Unit = {
    interpreter.set(interpDs, new NDArray[Array[Double]](
      model match {
        case Function(time: Time, _) =>
          if (time.isFormatted) { //text times
            val tf = time.timeFormat.get
            samples.map {
              case Sample(DomainData(Text(t)), RangeData(Number(n))) =>
                tf.parse(t) match {
                  case Right(v) => Array(v, n)
                }
            }.toArray.flatten
          } else { //numeric times
            val uc = UnitConverter(time.timeScale, TimeScale.Default)
            samples.map {
              case Sample(DomainData(Number(t)), RangeData(Number(n))) =>
                Array(uc.convert(t), n)
            }.toArray.flatten
          }
      }, samples.length, 2)
    )
  }

}

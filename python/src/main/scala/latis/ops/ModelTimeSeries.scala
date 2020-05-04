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
 * Defines an Operation that models a univariate time series
 * (i.e., time -> value) with a specified algorithm.
 * TODO: Should we avoid overloading the term "model"?
 */
trait ModelTimeSeries extends UnaryOperation {

  /** 
   * The name of the modeling algorithm (must not contain spaces).
   * Becomes the name of the new Dataset variable that holds the modeled time series values.
   */
  def modelAlg: String

  /** The type of the new Dataset variable. */
  def modelVarType: String

  /**
   * Path to the Python script that models the time series.
   */
  def pathToModelScript: String

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
            "id" -> modelAlg,
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
    assertNoSpaces(modelAlg)
    setJepPath
    
    val interp = new SharedInterpreter
    val samples = data.unsafeForce.sampleSeq

    try {
      interp.runScript(pathToModelScript)
      copyDataForPython(interp, model, samples)
      val modelOutputCol = runModelingAlgorithm(interp)

      //Reconstruct the SampledFunction with the modeled data included
      //TODO: is .zipWithIndex noticeably slower than a manual for loop?
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

  /**
   * Sets the path to the JEP library file if it hasn't already been set.
   * The file name is based off the operating system.
   */
  private def setJepPath: Unit = try {
    val path = {
      val os = System.getProperty("os.name").toLowerCase
      val fileName = {
        if (os.contains("win")) "jep.dll"
        else if (os.contains("mac")) "jep.cpython-36m-darwin.so"
        else "jep.cpython-36m-x86_64-linux-gnu.so"
      }
      System.getProperty("user.dir") + "/python/lib/" + fileName
    }
    MainInterpreter.setJepLibraryPath(path)
  } catch {
    case _: JepException => //JEP library path already set
  }

  /**
   * Throws an exception if the String contains a space.
   */
  private def assertNoSpaces(str: String): Unit = {
    if (str.contains(" ")) throw new RuntimeException(s"'$str' must not contain spaces.")
  }
  
}

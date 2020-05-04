package latis.ops

import jep.JepException
import jep.MainInterpreter
import latis.data.SampledFunction
import latis.model.DataType

/**
 * Defines an Operation that uses JEP and acts on a single Dataset.
 */
trait JepOperation extends UnaryOperation {

  /**
   * Provides a new model resulting from this Operation.
   */
  def applyToModel(model: DataType): DataType

  /**
   * Provides new Data resulting from this Operation.
   */
  def applyToData(data: SampledFunction, model: DataType): SampledFunction

  /**
   * Sets the path to the JEP library file if it hasn't already been set.
   * The file name is based off the operating system.
   */
  def setJepPath: Unit = try {
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

}

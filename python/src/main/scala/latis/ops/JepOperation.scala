package latis.ops

import jep.JepException
import jep.MainInterpreter

import latis.util.LatisConfig
import latis.util.LatisException

/**
 * Defines an Operation that uses JEP and acts on a single Dataset.
 */
trait JepOperation extends UnaryOperation {

  /**
   * Sets the path to the JEP library file if it hasn't already been set.
   * The file name is based off the operating system.
   */
  def setJepPath: Unit = try {
    val path = LatisConfig.get("latis.python.jep.path").getOrElse {
      val msg = "Path to the JEP native library file not found. " +
        "Please set an absolute path to it under 'latis.python.jep.path' in your config file."
      throw LatisException(msg)
    }
    MainInterpreter.setJepLibraryPath(path)
  } catch {
    case _: JepException => //JEP library path already set
  }

}

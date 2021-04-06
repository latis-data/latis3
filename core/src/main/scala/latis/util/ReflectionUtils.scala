package latis.util

import scala.reflect.runtime._

/**
 * Collection of utility methods for using reflection.
 */
object ReflectionUtils {
  /**
   * Return the java.lang.Class for the given a fully resolved class name.
   * This will NOT return an instance of that class.
   * This may throw an Exception.
   */
  //TODO: return Option or Try or IO?
  def getClassByName(className: String): Class[_] =
    Class.forName(className)

  /**
   * Construct a class given its fully resolved name.
   * Assumes there are no constructor arguments.
   */
  def constructClassByName(className: String): Any = {
    val cls = Class.forName(className)
    cls.getConstructor().newInstance()
  }

  /**
   * Get the companion object for the given class name.
   */
  def getCompanionObject(className: String): Any = {
    val moduleSymbol = currentMirror.staticModule(className)
    val moduleMirror = currentMirror.reflectModule(moduleSymbol)
    moduleMirror.instance
  }

  /**
   * Call a given methodName on the companion object of the given className
   * with the given arguments.
   */
  def callMethodOnCompanionObject(className: String, methodName: String, args: AnyRef*): Any = {
    /*
     * Notes:
     * - We need AnyRef for the args since it directly maps to Object for the method invocation.
     * - The class.getMethod appears to require an exact match on the parameter classes,
     *   not accounting for subclass relationships. Thus we do that test ourselves here.
     */
    val companionObject = getCompanionObject(className)
    val method = {
      val maybeMethod = companionObject.getClass().getMethods.find { method =>
        val argClasses   = args.map(_.getClass)     // types of args passed to method
        val paramClasses = method.getParameterTypes // types of method parameters
        (argClasses.length == paramClasses.length) && {
          (paramClasses.zip(argClasses)).forall {
            case (param, arg) =>
              param.isAssignableFrom(arg) // is arg a subclass of param
          }
        }
      }
      maybeMethod.getOrElse {
        val msg = s"Failed to find method $methodName on companion object $className."
        throw new RuntimeException(msg)
      }
    }

    method.invoke(companionObject, args: _*)
  }
}

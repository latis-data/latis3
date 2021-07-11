package latis.ops

import latis.data._
import latis.model.DataType
import latis.time.Time
import latis.util.LatisException

/**
 * Defines an abstract class with implementation to apply
 * an operation to all time variables as opposed to a single
 * targeted variable.
 */
abstract class TimeOperation extends MapOperation {
  //TODO: no-op shortcut if there are no Time variables in the dataset

  /**
   * Defines a data value converter based on the Time variable type.
   */
  protected def makeConverter(t: Time): Datum => Datum

  def mapFunction(model: DataType): Sample => Sample = {
    val converters: List[(SamplePosition, Datum => Datum)] = makeConverters(model)

    (sample: Sample) =>
      converters.foldRight(sample) { (c, s) =>
        val pos = c._1
        val convert = c._2
        s.getValue(pos) match {
          case Some(d: Datum) => s.updatedValue(pos, convert(d))
          case _ => throw LatisException("Model is inconsistent with the data.")
        }
      }
  }

  /**
   * Accumulates time variable positions and conversion functions
   */
  private def makeConverters(model: DataType): List[(SamplePosition, Datum => Datum)] =
    model.getScalars.collect {
      case t: Time =>
        val path = model.findPath(t.id).get //bug if path not found
        val pos = path.length match {
          case 0 => RangePosition(0) //hack for ConstantFunction
          case 1 => path.head
          case _ =>
            //TODO: support time in nested function
            val msg = "Time operation does not support times variables in nested Functions."
            throw new UnsupportedOperationException(msg)
        }
        (pos, makeConverter(t))
    }
}

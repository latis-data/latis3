package latis.data

import scala.math.Numeric._

trait Data extends Any

/*
 * TODO: Numeric
 * Numeric is limited, need Fractional to get div
 * consider using spire: rich mathematically correct types some with arbitrary precision
 * 
 * the numeric type (e.g. double vs float vs int) is separable from Time vs Wavelength...
 * there is no real need to allow others to extend the Data types
 * we can have a sealed trait of supported type
 * and use extension instead of type classes
 * extraction via pattern match might be handy (at least for testing), but spoils value class
 * 
 * How can we get Numeric support for the scala libs that use it? 
 * lots of things to impl, extend things like DoubleIsFractional?
 * 
 * Number => Double
 * Integer => Long
 * Real => Double
 * Text => String
 * Complex => (Double, Double)
 * 
 * SampledFunction extends Data
 *   but not Numeric or Ordering
 * 
 * 
 * extend specific type ordering? 
 *   Ordering.Double.IeeeOrdering
 *   Numeric extends Ordering so extending a Numeric instance might do it
 * 
 * have separate traits for domain vs range to prevent putting a SF in the domain?
 * DomainData = Vector[OrderedData]
 * RangeData = Vector[Data]
 * 
 * would PartiallyOrdered help?
 * just because data is numeric doesn't mean that that is the order we want
 * can we specify AsIsOrdering?
 * SF is not Ordered
 * compare(that: Data) not always defined
 * introduce Ordered for Number and Text only?
 * 
 * Note that binary search in IndexedFunction uses ScalarOrdering, can we get that from Ordered?
 */

/**
 * Define a trait for Data that can be ordered.
 * This is used to ensure that DomainData has an ordering.
 */
trait OrderedData extends Any with Data 
/*
 * Note, can't make ordering for any Data
 * only number and text makes sense to order
 * should we allow any combination of text and number to be ordered so we can have a single ordering?
 */

trait Number extends Any with OrderedData with Ordered[Number] {
  def doubleValue: Double
}
object Number {
  //Treat all Number as Doubles, for now
  def unapply(data: Number): Option[Double] = Option(data.doubleValue)
}

trait Index extends Number {
  def intValue: Int
  def doubleValue = intValue.toDouble
}
object Index {
  def unapply(data: Index): Option[Int] = Option(data.intValue)
}

trait Integer extends Number {
  def longValue: Long
  def doubleValue = longValue.toDouble
}
object Integer {
  def unapply(data: Integer): Option[Long] = Option(data.longValue)
}

trait Real extends Number
object Real {
  def unapply(data: Real): Option[Double] = Option(data.doubleValue)
}

trait Text extends Any with OrderedData with Ordered[Text] {
  def stringValue: String
}
object Text {
  def unapply(data: Text): Option[String] = Option(data.stringValue)
}

trait BinaryData extends Any with Data {
  def bytes: Array[Byte]
}
object BinaryData {
  def unapply(data: BinaryData): Option[Array[Byte]] = Option(data.bytes)
}

object Data {
  //Note, these are implicit so we can construct DomainData from primitive types
  //TODO: put in package object?
  implicit class DoubleValue(val value: Double) 
    extends AnyVal with Number {
      def doubleValue: Double = value
      def compare(that: Number): Int = 
        doubleValue compare that.doubleValue
  }
  
  implicit class IntValue(val value: Int) 
    extends AnyVal with Number {
      def doubleValue: Double = value.toDouble
      def compare(that: Number): Int = 
        doubleValue compare that.doubleValue
  }
  
  implicit class StringValue(val value: String) 
    extends AnyVal with Text {
      def stringValue: String = value
      def compare(that: Text): Int = 
        stringValue compare that.stringValue
  }

}
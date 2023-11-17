package latis.data

import cats.effect.IO
import cats.syntax.all._
import cats.kernel.Monoid
import fs2.Stream

import latis.model.DataType
import latis.ops.UnaryOperation
import latis.util.LatisException
import latis.util.StreamUtils

/**
 * The Data trait is the root of all data values that go into a Sample.
 */
sealed trait Data extends Any {

  /**
   * Returns this Data object as a Stream of Samples.
   */
  def samples: Stream[IO, Sample]

  /**
   * Evaluate this Data object at the given DomainData.
   */
  def eval(data: DomainData): Either[LatisException, RangeData]

  // Allows smart Data (e.g. RddFunction) to apply operations
  def applyOperation(
    op: UnaryOperation,
    model: DataType
  ): Either[LatisException, Data] =
    op.applyToData(this, model) //default when special SF can't apply op
}

//==== TupleData ===============================================================

class TupleData private (val elements: Seq[Data]) extends Data {
  def samples: Stream[IO, Sample] = Stream.emit {
    Sample(DomainData(), RangeData(elements))
  }

  def eval(data: DomainData): Either[LatisException, RangeData] = {
    if (data.isEmpty) RangeData(elements).asRight
    else LatisException("TupleData evaluation requires a zero-arity argument.").asLeft
  }
}

object TupleData {
  //TODO: prevent tuple of 0 or 1?
  //TODO: allow construction via fromSeq only?
  //Note, flatten prevents nested TupleData
  def apply(ds: Seq[Data]): TupleData = new TupleData(Data.flatten(ds))
  def apply(d: Data, ds: Data*): TupleData = TupleData(d +: ds)
  def unapplySeq(td: TupleData): Option[Seq[Data]] =
    Option(td.elements)
}

//==== SampledFunction =========================================================

trait SampledFunction extends Data {
  def ordering: Option[PartialOrdering[DomainData]]

  def resample(domainSet: DomainSet): Either[LatisException, SampledFunction] =
    domainSet.elements.toVector.traverse(eval).map { range =>
      SetFunction(domainSet, range)
    }


  def unsafeForce: MemoizedFunction = this match { //TODO: Either
      //TODO: optional to: FunctionFactory arg?
    case mf: MemoizedFunction => mf
    case _ => SampledFunction(StreamUtils.unsafeStreamToSeq(samples))
  }
}

object SampledFunction {
  def apply(stream: Stream[IO, Sample]): StreamFunction =
    new StreamFunction(stream)
  def apply(samples: Seq[Sample]): MemoizedFunction =
    new SeqFunction(samples)
  def unapply(sf: SampledFunction): Option[Stream[IO, Sample]] =
    Option(sf.samples)

  /** Defines a Monoid instance for SampledFunction */
  implicit val sampledFunctionMonoid: Monoid[SampledFunction] =
    new Monoid[SampledFunction] {
      def empty: SampledFunction = SeqFunction(Seq.empty)
      def combine(sf1: SampledFunction, sf2: SampledFunction): SampledFunction =
        SampledFunction(sf1.samples ++ sf2.samples)
    }
}

//==============================================================================

/*
 * Note: traits used for two purposes:
 *
 * - mixin properties
 * For example, Integer, which must be able to provide a Long,
 * is-a Number which must be able to provide a Double.
 * Thus, any data that is a subtype of Integer must be able to
 * provide both Long or Double.
 *
 * - pattern matching
 * For example, a value created as a subtype of Integer will match
 * Number (to extract a Double) or Integer (to extract a Long) but
 * won't match Real. Likewise, a Real can match Number but not Integer.
 * If you simply want to consume a data value as a number, match
 * on Number and use the Double value.
 * Neither would match Text even if the string value is "numeric".
 *
 * Datum is extended by traits Number and Text, and by value classes BooleanValue, BinaryValue,
 * BigIntValue, and BigDecimalValue.
 *
 * Number is extended by traits Integer and Real, while Text is extended by value classes CharValue
 * and StringValue.
 *
 * Integer is extended by trait IndexDatum and value class LongValue, while Real is extended by value
 * classes FloatValue and DoubleValue.
 *
 * IndexDatum is extended by ByteValue, CharValue, ShortValue, and IntValue.
 *
 * To summarize, here is a tree representing the hierarchy: (Note: CharValue can be reached two different ways)
 * /Datum/
 * ├─ BigDecimalValue
 * ├─ BigIntValue
 * ├─ BinaryValue
 * ├─ BooleanValue
 * ├─ /Number/
 * │  ├─ /Integer/
 * │  │  ├─ LongValue
 * │  │  ├─ /IndexDatum/
 * │  │  │  ├─ ByteValue
 * │  │  │  ├─ CharValue
 * │  │  │  ├─ IntValue
 * │  │  │  ├─ ShortValue
 * │  ├─ /Real/
 * │  │  ├─ DoubleValue
 * │  │  ├─ FloatValue
 * ├─ /Text/
 * │  ├─ CharValue
 * │  ├─ StringValue
 */

trait Datum extends Any with Data {
  def value: Any

  def samples: Stream[IO, Sample] = Stream.emit {
    Sample(DomainData(), RangeData(this))
  }

  def eval(data: DomainData): Either[LatisException, RangeData] = {
    if (data.isEmpty) RangeData(this).asRight
    else LatisException("Datum evaluation requires a zero-arity argument.").asLeft
  }

  def asString: String = value.toString
}

/**
 * Defines Data that can be used as a placeholder for
 * missing or otherwise invalid data.
 */
object NullData extends Data with Serializable {
  //TODO: does Data really need these?
  def samples: Stream[IO, (DomainData, RangeData)] = ???
  def eval(data: DomainData): Either[LatisException, RangeData] = ???

  override def toString: String = "null"
}

/**
 * Define a base trait for all numeric data.
 * Implementers of Number must be able to provide values as various
 * primitive numeric types.
 * Pattern matching on Number will extract a Double value.
 * This does not include String representations of a numeric value.
 */
trait Number extends Any with Datum {
  def asDouble: Double
}
object Number {
  // Extract a Double from a Number
  def unapply(data: Number): Option[Double] = Option(data.asDouble)
}

/**
 * Integer is a Number type representing integral data.
 * Pattern matching on Integer will extract a Long.
 */
trait Integer extends Any with Number {
  def asLong: Long
  def asDouble: Double = asLong.toDouble
}
object Integer {
  // Extract a Long from an Integer
  def unapply(data: Integer): Option[Long] = Option(data.asLong)
}

/**
 * IndexDatum is an Integer type representing data that can be used
 * to index a Seq or Array. Pattern matching on IndexDatum will
 * extract an Int.
 */
trait IndexDatum extends Any with Integer {
  def asInt: Int
  def asLong: Long = asInt.toLong
  override def asDouble: Double = asInt.toDouble
}
object IndexDatum {
  // Extract an Int from an IndexDatum
  def unapply(data: IndexDatum): Option[Int] = Option(data.asInt)
}

/**
 * Real is a Number type which can be used in a
 * pattern match to extract a Double.
 */
trait Real extends Any with Number
object Real {
  // Extract a Double from a Real
  def unapply(data: Real): Option[Double] = Option(data.asDouble)
}

/**
 * Text is a type of Data whose value is represented as a String.
 */
trait Text extends Any with Datum
object Text {
  // Extract a String from a Text
  def unapply(data: Text): Option[String] = Option(data.asString)
}

//=============================================================================

object Data {

  def fromSeq(ds: Seq[Data]): Data = {
    val flatData = flatten(ds)
    flatData.length match {
      case 0 => NullData
      case 1 => ds.head
      case _ => TupleData(ds)
    }
  }

  /**
   * Flattens TupleData within a given List of Data.
   * This is used to ensure that TupleData, DomainData,
   * and RangeData contain no TupleData.
   */
  def flatten(ds: Seq[Data]): List[Data] = ds.toList match {
    case Nil => Nil
    case d :: Nil => d match {
      case TupleData(ds @ _*) => ds.toList
      case d: Data => List(d) //Datum or SF
    }
    case d :: ds =>d match {
      case TupleData(es @ _*) => es.toList ++ flatten(ds)
      case d: Data => d +: flatten(ds) //Datum or SF
    }
  }

  /**
   * Tries to construct a Datum from any value.
   */
  //TODO: consider type class Datum[A] so anyone could make an instance
  //  Data.fromValue(v: A)(implicit ev: ?)
  def fromValue(v: Any): Either[LatisException, Datum] = v match {
    case x: Boolean     => Right(BooleanValue(x))
    case x: Byte        => Right(ByteValue(x))
    case x: Char        => Right(CharValue(x))
    case x: Short       => Right(ShortValue(x))
    case x: Int         => Right(IntValue(x))
    case x: Long        => Right(LongValue(x))
    case x: Float       => Right(FloatValue(x))
    case x: Double      => Right(DoubleValue(x))
    case x: String      => Right(StringValue(x))
    case x: Array[Byte] => Right(BinaryValue(x))
    case x: BigInt      => Right(BigIntValue(x))
    case x: BigDecimal  => Right(BigDecimalValue(x))
    case _ => Left(LatisException(s"Can't make a Datum from the value $v"))
  }

  /**
   * Defines a Datum that can hold any object.
   * This is largely to make the Data constructor total
   * but it could be handy, for example, when you have a
   * time series of objects.
   * Note that this is not implicit like other Datum subclasses.
   */
  //case class AnyValue(value: Any) extends Datum

  //Note, these are value classes
  //Note, these are implicit so we can construct DomainData from primitive types
  //  Import latis.data.Data._ to get implicit Data construction from supported types
  //Note, if we change the Numeric traits here (e.g. is Char really a Number) make sure
  //  the ValueTypes are consistent.

  implicit class BooleanValue(val value: Boolean) extends AnyVal with Datum with Serializable {
    override def toString = s"BooleanValue($value)"
  }

  implicit class ByteValue(val value: Byte) extends AnyVal with IndexDatum with Serializable {
    def asInt: Int = value.toInt
    override def toString = s"ByteValue($value)"
  }

  // single Text character or Integer 0-65535
  implicit class CharValue(val value: Char) extends AnyVal with Text with IndexDatum with Serializable {
    def asInt: Int = value.toInt
    override def toString = s"CharValue($value)"
    //TODO: numeric or single test character?
  }

  implicit class ShortValue(val value: Short) extends AnyVal with IndexDatum with Serializable {
    def asInt: Int = value.toInt
    override def toString = s"ShortValue($value)"
  }

  implicit class IntValue(val value: Int) extends AnyVal with IndexDatum with Serializable {
    def asInt: Int = value
    override def toString = s"IntValue($value)"
  }

  implicit class LongValue(val value: Long) extends AnyVal with Integer with Serializable {
    def asLong: Long = value
    override def toString = s"LongValue($value)"
  }

  implicit class FloatValue(val value: Float) extends AnyVal with Real with Serializable {
    def asDouble: Double = value.toDouble
    override def toString = s"FloatValue($value)"
  }

  implicit class DoubleValue(val value: Double) extends AnyVal with Real with Serializable {
    def asDouble: Double = value
    override def toString = s"DoubleValue($value)"
  }

  implicit class StringValue(val value: String) extends AnyVal with Text with Serializable {
    override def toString = s"StringValue($asString)"
  }

  implicit class BinaryValue(val value: Array[Byte]) extends AnyVal with Datum with Serializable {
    private[data] def len2str(length: Int): String = {
      length.toString + " Byte Binary"
    }
    override def asString: String = len2str(value.length)
    override def toString = s"BinaryValue($asString)"
  }

  implicit class BigIntValue(val value: BigInt) extends AnyVal with Datum with Serializable {
    override def toString = s"BigIntValue($value)"
  }

  implicit class BigDecimalValue(val value: BigDecimal) extends AnyVal with Datum with Serializable {
    override def toString = s"BigDecimalValue($value)"
  }
}

package latis.data

import scala.util.Try

/**
 * Define the algebraic data type for data that are represented by the
 * Functional Data Model.
 */
sealed trait Data extends Ordered[Data] {
  
  /**
   * Implement the compare method for the Ordered trait.
   * This is generally used to compare domain values of
   * Samples within the same Function so it is expected 
   * that they will be of the same type.
   */
   //TODO: Consider cross-comparing types if the values
   //  are equivalent. Note implications on "equals".
  def compare(that: Data): Int = (this, that) match {
    case (BooleanData(a), BooleanData(b)) => a compare b
    case (ByteData(a), ByteData(b))       => a compare b
    case (CharData(a), CharData(b))       => a compare b
    case (DoubleData(a), DoubleData(b))   => a compare b
    case (FloatData(a), FloatData(b))     => a compare b
    case (IntData(a), IntData(b))         => a compare b
    case (LongData(a), LongData(b))       => a compare b
    case (Integer(a), Integer(b))         => a compare b
    case (Real(a),Real(b))                => a compare b
    case (ShortData(a), ShortData(b))     => a compare b
    case (Text(a), Text(b))               => a compare b
    case (Index(a), Index(b))             => a compare b
    //TODO: other permutations? or assume only between values in domain of same type?
    
    case (TupleData(as @ _*), TupleData(bs @ _*)) =>
      //TODO: flatten?
      if (as.length == bs.length) comparePairs(as zip bs)
      else throw new UnsupportedOperationException("Can't compare Tuples of different arity.")
      
    case _ => throw new UnsupportedOperationException(s"Can't compare $this and $that")
  }
    
  /**
   * Helper method to compare two sequences of Data recursively.
   * If the first pair matches recursively test the next pair.
   * Used to compare Tuples.
   */
  private def comparePairs(ps: Seq[(Data, Data)]): Int = ps.toList match {
    case Nil => 0
    case head :: tail => head._1 compare head ._2 match {
      case 0 => comparePairs(tail)  //recurse
      case c: Int => c
    }
  }
  
  /**
   * Flatten nested TupleData into a single TupleData.
   * ScalarData and FunctionData will remain unaffected.
   * Note that this is not implemented as an Operation
   * because we only want to flatten the Data and not the model.
   */
  def flatten: Data = {
    def go(data: Data, acc: Seq[Data]): Seq[Data] = data match {
      case TupleData(ds @ _*) => ds.map(go(_, acc)).flatten
      case d: Data => acc :+ d
    }
    
    val ds = go(this, Seq.empty)
    ds.length match {
      case 0 => TupleData() //only if we started with an empty Tuple
      case 1 => ds.head
      case _ => TupleData.fromSeq(ds)
    }
  }
}

//-- Scalar -----------------------------------------------------------------//

/**
 * ScalarData represents a single data value.
 */
case class ScalarData[+T](val value: T) extends Data

object ScalarData {
  
  def fromValue(value: Any) = value match {
    case v: Byte       => new ScalarData(v) with ByteData
    case v: Short      => new ScalarData(v) with ShortData
    case v: Int        => new ScalarData(v) with IntData
    case v: Long       => new ScalarData(v) with LongData
    case v: Float      => new ScalarData(v) with FloatData
    case v: Double     => new ScalarData(v) with DoubleData
    case v: Boolean    => new ScalarData(v) with BooleanData
    case v: Char       => new ScalarData(v) with CharData
    case v: BigInt     => new ScalarData(v) with Integer
    case v: BigDecimal => new ScalarData(v) with Real
    case v: String     => new ScalarData(v) with Text
    case v @ (_: Double, _: Double) => new ScalarData(v) with ComplexData
    case _             => new ScalarData(value)
  }
  
  /*
   * TODO value plus type string from metadata?
   * sd.asType("real") = new ScalarData(sd.value) with Real ?
   */
}

//-- Tuple ------------------------------------------------------------------//

/**
 * TupleData is a container of zero or more Data objects.
 */
case class TupleData(elements: Data*) extends Data

object TupleData {
  def fromSeq(elements: Seq[Data]): TupleData = TupleData(elements: _*)
}

//-- Function ---------------------------------------------------------------//

/**
 * SampledFunction represent a (potentially lazy) ordered sequence of Samples.
 */
trait SampledFunction extends Data {
  def samples: Iterator[Sample]
}

object SampledFunction {
  
  /**
   * Extract an Iterator of Samples from FunctionData.
   */
  def unapply(sf: SampledFunction): Option[Iterator[Sample]] = Option(sf.samples)
}


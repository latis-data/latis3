package latis.model

import latis.data._
import latis.metadata._

import scala.collection.TraversableLike
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Builder
import scala.collection.mutable.Stack
import scala.util.Failure
import scala.util.Success

/**
 * Define the algebraic data type for the LaTiS implementation of the 
 * Functional Data Model.
 */
sealed trait DataType 
  extends Traversable[DataType] 
  with TraversableLike[DataType, DataType] 
  with MetadataLike {
    
  override protected[this] def newBuilder: Builder[DataType, DataType] = DataType.newBuilder
  
  /**
   * Experimental implementation of Traversable
   * to support various model operations.
   */
  def foreach[U](f: DataType => U): Unit = {
    // Recursive helper function, depth first
    // Only safe for operating on Scalars?
    def go(v: DataType): Unit = {
      v match {
        case _: Scalar   => //end of this branch
        case Tuple(vs @ _*) => vs.map(go(_)) //recurse
        case Function(d,r)  => go(d); go(r)  //recurse
      }
      //apply function after taking care of kids = depth first
      f(v)
    }
    go(this)
  }
  
  /**
   * Return a List of Scalars in the (depth-first) order 
   * that they appear in the model.
   */
  def getScalars: List[Scalar] = toList.collect { case s: Scalar => s }
  
  /**
   * Get the DataType of a variable by its identifier.
   */
  def getVariable(id: String): Option[DataType] = find(_.id == id)
  
  /**
   * Find the DataType of a variable by its identifier or aliases.
   */
  def findVariable(variableName: String): Option[DataType] = find(_.id == variableName)
  //TODO: support aliases
  
  /**
   * Return the function arity of this DataType.
   * For Function, this is the number of top level types (non-flattened) 
   * in the domain. 
   * For Scalar and Tuple, there is no domain so the arity is 0.
   */
  def arity: Int = this match {
    case Function(domain, _) => domain match {
      case _: Scalar => 1
      case t: Tuple => t.elements.length
      case _: Function => ??? //deal with Function in the domain, or disallow it
    }
    case _ => 0
  }
  
  // Used by Rename Operation
  def rename(name: String): DataType = this match {
    //TODO: add old name to alias?
    case _: Scalar => Scalar(metadata + ("id" -> name))
    case Tuple(es @ _*) => Tuple(metadata + ("id" -> name), es: _*)
    case Function(d, r) => Function(metadata + ("id" -> name), d, r)
  }
    
  /**
   * Return this DataType with all nested Tuples flattened to a single Tuple.
   * A Scalar will remain a Scalar.
   * This form is consistent with Samples which don't preserve nested Functions.
   */
  def flatten(): DataType = {
    // Recursive helper function that uses an accumulator (acc)
    // to accumulate types while in the context of a Tuple
    // while the recursion results build up the final type.
    def go(dt: DataType, acc: Seq[DataType]): Seq[DataType] = dt match {
      case s: Scalar => acc :+ s
      case Tuple(es @ _*) => es.flatMap(e => acc ++ go(e, Seq()))
      case Function(d, r) => acc :+ Function(d.flatten(), r.flatten())
    }
    
    val types = go(this, Seq())
    types.length match {
      case 1 => types.head
      case n => Tuple(types: _*)
    }
  }

/**
 * Return the path within this DataType to a given variable ID
 * as a sequence of SamplePositions.
 * Note that length of the path reflects the number of nested Functions.
 */
  def getPath(id: String): Option[SamplePath] = {
    
    // Recursive function to try paths until it finds a match
    def go(dt: DataType, id: String, currentPath: SamplePath): Option[SamplePath] = {
      if (id == dt.id) Some(currentPath) //found it  //TODO: use hasName to cover aliases?
      else dt match { //recurse
        case _: Scalar => None  //dead end
        case Function(dtype, rtype) =>
          val ds = dtype match {
            case s: Scalar => Seq(s)
            case Tuple(vs @ _*) => vs
          }
          val rs = rtype match {
            case Tuple(vs @ _*) => vs
            case _ => Seq(rtype)
          }
          
          val d = ds.indices.iterator map { i => // lazy Iterator allows us to short-circuit
            go(ds(i), id, currentPath :+ DomainPosition(i))
          }
          val r = rs.indices.iterator map { i => // lazy Iterator allows us to short-circuit
            go(rs(i), id, currentPath :+ RangePosition(i))
          }
          
          (d ++ r) collectFirst { case Some(p) => p } //short-circuit here, take first Some
      }
    }
    
    go(this.flatten(), id, Seq.empty)
  }

  //TODO: beef up
  //TODO: use missingValue in metadata, scalar.parseValue(s)
  def makeFillValues: RangeData = RangeData(getFillValue(this, Seq.empty))

  private def getFillValue(dt: DataType, acc: Seq[Data]): Seq[Data] = dt match {
    case s: Scalar => acc :+ s.valueType.fillValue
    case Tuple(es @ _*) => es.flatMap(getFillValue(_, acc))
    case _: Function =>
      val msg = "Can't make fill values for Function, yet."
      throw new RuntimeException(msg)
  }
}

//---------------------------------------------------------------------------//

object DataType {
    
  //Note, this will only work for a Seq with traversal defined by foreach
  def fromSeq(vars: Seq[DataType]): DataType = {
    def go(vs: Seq[DataType], hold: Stack[DataType]): DataType = {
      vs.headOption match {
        case Some(s: Scalar) => 
          go(vs.tail, hold.push(s)) //put on the stack then do the rest
        case Some(t: Tuple) => 
          val n = t.elements.length
          val t2 = Tuple(t.metadata, (0 until n).map(_ => hold.pop).reverse: _*)
          go(vs.tail, hold.push(t2))
        case Some(f: Function) => 
          val c = hold.pop
          val d = hold.pop
          val f2 = Function(f.metadata, d, c)
          go(vs.tail, hold.push(f2))
        case None => hold.pop //TODO: test that the stack is empty now
      }
    }
    go(vars, Stack())
  }

  def newBuilder: mutable.Builder[DataType, DataType] = new ArrayBuffer mapResult fromSeq

  implicit def canBuildFrom: CanBuildFrom[DataType, DataType, DataType] =
    new CanBuildFrom[DataType, DataType, DataType] {
      def apply(): mutable.Builder[DataType, DataType] = newBuilder
      def apply(from: DataType): mutable.Builder[DataType, DataType] = newBuilder
    }
}

//-- Scalar -----------------------------------------------------------------//

/**
 * The Scalar type represents a single atomic variable.
 */
class Scalar(val metadata: Metadata) extends DataType {
  //TODO: type safe units,...
  //TODO: use ScalarOps to clean up this file? in package?
  //TODO: construct with type ..., use smart constructor to build from Metadata,
  //      or type safe Metadata
  
  //import scala.math.Ordering._

  // Note, this will fail eagerly if constructing a Scalar
  // with an invalid value type.
  val valueType: ValueType = this("type").map {
    ValueType.fromName(_) match {
      case Right(v) => v
      case Left(e) => throw e
    }
  }.getOrElse {
    val msg = s"No value type defined for Scalar: $id"
    throw new RuntimeException(msg)
  }

  /**
   * Converts a string value into the appropriate type for this Scalar.
   */
  def parseValue(value: String): Either[Exception, Datum] =
    valueType.parseValue(value) match {
      case Success(data) => Right(data)
      case Failure(t) =>
        //TODO: use our own exception type here
        val e = new RuntimeException("Oops", t)
        Left(e)
    }

  /**
   * Returns a string representation of the given Datum.
   */
  def formatValue(data: Datum): String = data.asString

  /**
   * Converts a string value into the appropriate type and units
   * for this Scalar.
   */
  def convertValue(value: String): Either[Exception, Datum] =
    parseValue(value)
  //TODO: support units, e.g. "1.2 meters"

  def ordering: Ordering[Datum] = ???
  /**
   * Defines an Ordering for Data of the type described by this Scalar.
   * An IllegalArgumentException will be thrown if the compared Data types 
   * are not consistent with this Scalar.
   */
//  def ordering: Option[PartialOrdering[Data]] = valueType match {
//    case BooleanValueType =>
//    case ByteValueType =>
//    case CharValueType =>
//    case ShortValueType =>
//    case IntValueType => new PartialOrdering[Data] {
//      override def lteq(d1: Data, d2: Data): Boolean = ???
//      override def tryCompare(d1: Data, d2: Data): Option[Int] => (d1, d2) match {
//        case (d1: Data.IntValue, d2: Data.IntValue) =>
//          Some(scala.math.Ordering.Int.compare(d1.value, d2.value))
//        case _ => None
//      }
//    }
//
//    case LongValueType =>
//    case FloatValueType =>
//    case DoubleValueType =>
//    case BinaryValueType =>
//    case StringValueType =>
//    case BigIntValueType =>
//    case BigDecimalValueType =>
//  }
//
//  // Optimization to define Ordering only once
//  private lazy val _ordering = this("type") match {
//    case Some("short") => makeOrdering {
//      case (d1: Data.ShortValue, d2: Data.ShortValue) =>
//        Short.compare(d1.value, d2.value)
//    }
//    case Some("int") => makeOrdering {
//      case (d1: Data.IntValue, d2: Data.IntValue) =>
//        Int.compare(d1.value, d2.value)
//    }
//    case Some("long") => makeOrdering {
//      case (d1: Data.LongValue, d2: Data.LongValue) =>
//        Long.compare(d1.value, d2.value)
//    }
//    case Some("float") => makeOrdering {
//      case (d1: Data.FloatValue, d2: Data.FloatValue) =>
//        Float.compare(d1.value, d2.value)
//    }
//    case Some("double") => makeOrdering {
//      case (d1: Data.DoubleValue, d2: Data.DoubleValue) =>
//        Double.compare(d1.value, d2.value)
//    }
//    case Some("string") => makeOrdering {
//      case (d1: Data.StringValue, d2: Data.StringValue) =>
//        String.compare(d1.value, d2.value)
//    }
//    case Some(s) =>
//      val msg = s"Ordering not supported for data type $s"
//      throw new NotImplementedError(msg)
//  }
//
//  // Convenience method to avoid boilerplate duplication
//  private def makeSomePartialOrdering(part: PartialFunction[(Data,Data),Int]) = {
//    val error: PartialFunction[(Data,Data),Int] = {
//      case (x: Data, y: Data) =>
//        val msg = s"Incomparable data values: $x $y"
//        throw new IllegalArgumentException(msg)
//    }
//    val po: PartialOrdering[Data] = //part orElse ???
//      (d1: Data, d2: Data) =>
//      part(d1, d2)
//    new PartialOrdering[Data] {
//      def tryCompare(x: Data, y: Data): Option[Int] = (part orElse error)(x, y)
//    }
//  }

  override def toString: String = id
}

object Scalar {
  //TODO enforce id or make uid
  
  /**
   * Construct a Scalar with the given Metadata.
   */
  def apply(md: Metadata): Scalar = new Scalar(md)
  
  /**
   * Construct a Scalar with the given identifier.
   */
  def apply(id: String): Scalar = Scalar(Metadata(id))
}

//-- Tuple ------------------------------------------------------------------//

/**
 * A Tuple type represents a group of variables of other DataTypes.
 */
class Tuple(val metadata: Metadata, val elements: DataType*) extends DataType {
  
  override def toString: String = elements.mkString("(", ", ", ")")

}

object Tuple {
  //TODO enforce id or make uid
  
  /**
   * Construct a Tuple from Metadata and a comma separated list
   * of data types.
   */
  def apply(metadata: Metadata, elements: DataType*): Tuple = 
    new Tuple(metadata, elements: _*)
  
  /**
   * Construct a Tuple with default Metadata.
   */
  def apply(elements: DataType*): Tuple = 
    Tuple(Metadata(), elements: _*)
    
  /**
   * Extract elements of a Tuple with a comma separated list
   * as opposed to a single Seq.
   */
  def unapplySeq(tuple: Tuple): Option[Seq[DataType]] = 
    Option(tuple.elements)
}

//-- Function ---------------------------------------------------------------//

/**
 * A Function type represents a functional mapping 
 * from the domain type to the range type.
 */
class Function(val metadata: Metadata, val domain: DataType, val range: DataType) 
  extends DataType {
  
  override def toString: String = s"$domain -> $range"
}

object Function {
  //TODO enforce id or make uid
  
  /**
   * Construct a Function from metadata and the domain and range types.
   */
  def apply(metadata: Metadata, domain: DataType, range: DataType): Function = 
    new Function(metadata, domain, range)
  
  /**
   * Construct a Function with default Metadata.
   */
  def apply(domain: DataType, range: DataType): Function = 
    Function(Metadata(), domain, range)
    
  /**
   * Construct a Function from a Seq of domain variables
   * and a Seq of range variables. This will make a wrapping
   * Tuple only if the Seq has more that one element.
   */
  //TODO: Use Index if one is empty, error if both are empty?
  def apply(ds: Seq[DataType], rs: Seq[DataType]): Function = {
    val domain = ds.length match {
      case 0 => ??? //TODO: Index
      case 1 => ds.head
      case _ => Tuple(ds: _*)
    }
    val range = rs.length match {
      case 0 => ??? //TODO: no range, make domain a function of Index
      case 1 => rs.head
      case _ => Tuple(rs: _*)
    }
    
    Function(domain, range)
  }
  
  /**
   * Extract the domain and range types from a Function as a pair.
   */
  def unapply(f: Function): Option[(DataType, DataType)] = 
    Option((f.domain, f.range))
    
}

package latis.model

import latis.metadata._
import latis.data._
import scala.collection.TraversableLike
import scala.collection.mutable.Builder
import scala.collection.mutable.Stack
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ArrayBuffer

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
   * Return a Vector of Scalars in the (depth-first) order 
   * that they appear in the model.
   */
  def getScalars: Vector[Scalar] = toVector.collect { case s: Scalar => s }
  
  /**
   * Find the DataType of a variable by its identifier.
   */
  def findVariable(id: String): Option[DataType] = find(_.id == id)
  
  /**
   * Return the function arity of this DataType.
   * For Function, this is the number of top level types (non-flattened) 
   * in the domain. 
   * For Scalar and Tuple, there is no domain so the arity is 0.
   */
  def arity = this match {
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
          
          val d = (0 until ds.length).iterator map { i => // lazy Iterator allows us to short-circuit
            go(ds(i), id, currentPath :+ DomainPosition(i))
          }
          val r = (0 until rs.length).iterator map { i => // lazy Iterator allows us to short-circuit
            go(rs(i), id, currentPath :+ RangePosition(i))
          }
          
          (d ++ r) collectFirst { case Some(p) => p } //short-circuit here, take first Some
      }
    }
    
    go(this.flatten, id, Seq.empty)
  }

}

object DataType {
    
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
  
  def newBuilder: Builder[DataType, DataType] = new ArrayBuffer mapResult fromSeq

  implicit def canBuildFrom: CanBuildFrom[DataType, DataType, DataType] =
    new CanBuildFrom[DataType, DataType, DataType] {
      def apply(): Builder[DataType, DataType] = newBuilder
      def apply(from: DataType): Builder[DataType, DataType] = newBuilder
    }
}

//-- Scalar -----------------------------------------------------------------//

/**
 * The Scalar type represents a single atomic variable.
 */
class Scalar(val metadata: Metadata) extends DataType {
  
  /**
   * Convert a string value into the appropriate type for this Scalar.
   */
  def parseValue(value: String): Any = this("type") match {
    //TODO: deal with parse errors
    //TODO: use enumeration, ADT, fdml schema
    case Some("boolean")    => value.toBoolean
    case Some("char")       => value.head
    case Some("short")      => value.toShort
    case Some("int")        => value.toInt
    case Some("long")       => value.toLong
    case Some("float")      => value.toFloat
    case Some("double")     => value.toDouble
    case Some("string")     => value
    case Some("bigInt")     => BigInt(value)
    case Some("bigDecimal") => BigDecimal(value)
    //TODO: binary blob
    //TODO: class, e.g. latis.time.Time?
    case Some(s) => ??? //unsupported type s
    case None => ??? //type not defined
  }
  
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

package latis.model

import latis.metadata._

/**
 * Define the algebraic data type for the LaTiS implementation of the 
 * Functional Data Model.
 */
sealed trait DataType extends Traversable[DataType] with MetadataLike {
    
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
    
}

//-- Scalar -----------------------------------------------------------------//

/**
 * The Scalar type represents a single atomic variable.
 */
class Scalar(val metadata: Metadata) extends DataType {
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
  
  /**
   * Return the number of elements in this Tuple.
   * Note that it will only count top level elements,
   * not nested ones.
   */
  def arity: Int = elements.length
  
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
   * Extract the domain and range types from a Function as a pair.
   */
  def unapply(f: Function): Option[(DataType, DataType)] = 
    Option((f.domain, f.range))
    
}

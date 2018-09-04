package latis.model

import latis.metadata._

/**
 * Define the algebraic data type for the LaTiS implementation of the 
 * Functional Data Model.
 */
sealed trait DataType extends Traversable[DataType] with MetadataLike {
    
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
  
  def getScalars: Vector[Scalar] = toVector.collect { case s: Scalar => s }
    
  def id: String = metadata.getProperty("id", "")
}

//-- Scalar -----------------------------------------------------------------//

/**
 * The Scalar type represents a single atomic variable.
 */
class Scalar(val metadata: Metadata) extends DataType {
  override def toString: String = metadata.id
}

object Scalar {
  //TODO enforce id or uid
  def apply(id: String): Scalar = new Scalar(Metadata(id))
}

//-- Tuple ------------------------------------------------------------------//

/**
 * A Tuple type represents a group of other DataTypes.
 */
//case class Tuple(metadata: Metadata, elements: Seq[DataType]) extends DataType
class Tuple(val metadata: Metadata, val elements: DataType*) extends DataType {
  override def toString: String = elements.mkString("(", ", ", ")")
}

object Tuple {
  def apply(metadata: Metadata, elements: DataType*): Tuple = 
    new Tuple(metadata, elements: _*)
  def apply(elements: DataType*): Tuple = 
    new Tuple(Metadata(), elements: _*)
  def unapplySeq(tuple: Tuple): Option[Seq[DataType]] = 
    Option(tuple.elements)
}

//-- Function ---------------------------------------------------------------//

/**
 * A Function type represents a functional mapping from the domain type to the range type.
 */
class Function(val metadata: Metadata, val domain: DataType, val range: DataType) extends DataType {
  override def toString: String = s"$domain -> $range"
}

object Function {
  def apply(metadata: Metadata, domain: DataType, range: DataType) = new Function(metadata, domain, range)
  def apply(domain: DataType, range: DataType) = new Function(Metadata(), domain, range)
  def unapply(f: Function): Option[(DataType, DataType)] = Some((f.domain, f.range))
}

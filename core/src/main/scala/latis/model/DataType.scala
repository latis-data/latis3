package latis.model

import latis.metadata._

/**
 * Define the algebraic data type for the LaTiS implementation of the 
 * Functional Data Model.
 */
sealed abstract class DataType(metadata: Metadata) extends MetadataLike {
  
  def id: String = metadata.getProperty("id", "")
}

//-- Scalar -----------------------------------------------------------------//

/**
 * The Scalar type represents a single atomic variable.
 */
case class Scalar(metadata: Metadata) extends DataType(metadata)

object Scalar {
  
  def apply(id: String): Scalar = Scalar(Metadata("id" -> id))
}

//-- Tuple ------------------------------------------------------------------//

/**
 * A Tuple type represents a group of other DataTypes.
 */
case class Tuple(metadata: Metadata, elements: Seq[DataType]) extends DataType(metadata)

object Tuple {

  def unapplySeq(tuple: Tuple): Option[Seq[DataType]] = Option(tuple.elements)
}

//-- Function ---------------------------------------------------------------//

/**
 * A Function type represents a functional mapping from the domain type to the range type.
 */
case class Function(metadata: Metadata, domain: DataType, range: DataType) extends DataType(metadata)

object Function {
  
  def unapply(f: Function): Option[(DataType, DataType)] = Some((f.domain, f.range))
}

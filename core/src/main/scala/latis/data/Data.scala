package latis.data

import scala.util.Try

/**
 * Define the algebraic data type for data that are represented by the
 * Functional Data Model.
 */
sealed trait Data

//-- Scalar -----------------------------------------------------------------//

/**
 * ScalarData represents a single data value.
 */
case class ScalarData[+T](value: T) extends Data

//-- Tuple ------------------------------------------------------------------//

/**
 * TupleData is a container of other Data objects.
 */
case class TupleData(d: Data, ds: Data*) extends Data with Seq[Data] {
  // Note: we use this argument list definition to enforce that
  //   TupleData has at least one element. This also gives us the desired
  //   unapplySeq behavior for pattern match extraction.
  //   It also disambiguates the Seq[Data] arg.
  
  // Combine the Data into a single sequence of Data elements
  val elements: Seq[Data] = d +: ds
  
  // Implement Seq methods
  def apply(index: Int): Data = elements(index)
  def iterator: Iterator[Data] = elements.iterator
  def length: Int = elements.length
}

object TupleData {
  
  /**
   * Construct TupleData from a sequence of Data.
   */
  def apply(data: Seq[Data]): TupleData = data.length match {
    case 0 => ??? //TODO: error if empty
    case _ => TupleData(data.head, data.tail: _*)
  }
}

//-- Function ---------------------------------------------------------------//

/**
 * FunctionData represent a (potentially lazy) ordered sequence of Samples.
 * FunctionData may also be evaluated with support for exceptions.
 */
trait FunctionData extends Data {

  def samples: Iterator[Sample]
  
  def apply(v: Data): Try[Data]
}

object FunctionData {
  
  /**
   * Extract an Iterator of Samples from FunctionData.
   */
  def unapply(fd: FunctionData): Option[Iterator[Sample]] = Option(fd.samples)
}

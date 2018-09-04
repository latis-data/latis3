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
 * TupleData is a container of zero or more Data objects.
 */
case class TupleData(elements: Data*) extends Data

object TupleData {
  def fromSeq(elements: Seq[Data]): TupleData = TupleData(elements: _*)
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

package latis.model

import latis.data.Data
import latis.metadata.Metadata
import latis.units.MeasurementScale
import latis.util.Identifier

sealed trait DataType extends DataTypeAlgebra
  //TODO: enforce uniqueness
  //TODO: support qualified identifiers
  //TODO: no Index in stand-alone (constant) Scalar or Tuple
  //  can only know when assigned to a Dataset?

//---- Scalar ----//

//TODO: limit construction
//  need protected so Time can extend it
//  need private[model] so ScalarFactory can construct it
class Scalar(
  val metadata: Metadata,
  val id: Identifier,
  val valueType: ValueType,
  val units: Option[String] = None,
  val scale: Option[MeasurementScale] = None,
  val missingValue: Option[Data] = None,
  val fillValue: Option[Data] = None,
  val precision: Option[Int] = None,
  val ascending: Boolean = true
) extends DataType with ScalarAlgebra
  //TODO: coverage, cadence, resolution, start, end,... (see fdml schema)

object Scalar extends ScalarFactory

//---- Tuple ----//

class Tuple private[model](val id: Option[Identifier], e1: DataType, e2: DataType, es: DataType*) extends DataType {

  /**
   * Returns a list of the Tuple elements which is guaranteed to have at least two elements.
   */
  def elements: List[DataType] = e1 +: (e2 +: es.toList)

  /** Returns a list of Tuple elements with no nested Tuples. */
  def flatElements: List[DataType] = {
    def go(dt: DataType): List[DataType] = dt match {
      case t: Tuple => t.elements.flatMap(go)
      case _        => List(dt)
    }
    go(this)
  }

  override def toString: String = id.map(_.asString +": ").getOrElse("") + elements.mkString("(", ", ", ")")
}

object Tuple extends TupleFactory {

  //Note: case Tuple(e1, e2, es @ _*) not considered exhaustive
  //  favor case t: Tuple => t.elements ...
  def unapplySeq(tuple: Tuple): Some[Seq[DataType]] = Some(tuple.elements)
}

//---- Function ----//

class Function private[model](val id: Option[Identifier], val domain: DataType, val range: DataType) extends DataType {

  override def toString: String = id.map(_.asString +": ").getOrElse("") + s"$domain -> $range"
}

object Function extends FunctionFactory {

  def unapply(f: Function): Some[(DataType, DataType)] = Some((f.domain, f.range))
}

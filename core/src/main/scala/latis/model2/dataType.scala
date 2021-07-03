package latis.model2

import cats.syntax.all._

import latis.data.Data
import latis.model.ValueType
import latis.util.Identifier
import latis.util.LatisException

// See DataTypeOps
sealed trait DataType
  //TODO: enforce uniqueness
  //TODO: support qualified identifiers
  //TODO: no Index in stand-alone (constant) Scalar or Tuple?

  //def id: Option[Identifier]? NO
  //  only Projection, Rename look for id for any type, will need attention with qid
  //def scalars: List[Scalar] = ??? //needed? do we just need qualified ids for each scalar?
  //  generally used to zip with sample values
  //  nonIndexScalars?
  //  often a crutch without consideration for Functions in Tuples
  //  TODO: use this as an opportunity to sanity check assumptions about tuples and functions in tuples
  //def flatten: DataType? NO
  //  used on Tuple only: GroupByVariable, TimeTupleToTime
  //    but nested tuples not usually considered
  //  we often use getScalars to avoid nesting issues, not safe for Functions in Tuples
  //  use Tuple.flatElements
  //def rename or withId
  //  Pivot, Rename, DataType.flatten
  //  Note need copy constructor that subclasses can override so we preserve subclass
  //  TODO: need to enforce uniqueness but don't have scope
  //    smells like job for operation but need to be able to preserve scalar subclass
  //    do we need to reconstruct from metadata with class?
  //arity? vs dimensionality? (#56)
  //  DatasetGenerator, Curry, Evaluation, Resample
  //  note getScalars makes arity useless for nested tuples, though we do so buggily
  //  TODO: use arity as an opportunity to root out bugs
  /*
  TODO: map?
    ConvertTime.scala:56
      Time => Time
    FormatTime.scala:57
      Time => Time
    Rename.scala:20
      any DT to same type
    TimeTupleToTime.scala:21
      Tuple => Time
    not generally safe for any type mapping, changes structure that we are traversing over
    limit to Scalar => Scalar? TTTT is on its own?
    substitution use case? uri => Function
    special cases don't need general solution here, but might be handy abstraction that opens doors?
   */


object DataType {

  // Deal with Tuple arity < 2 complications
  def fromSeq(dts: Seq[DataType]): Either[LatisException, DataType] =
    dts.length match {
      case 0 => LatisException("Empty DataType not supported").asLeft
      case 1 => dts.head.asRight
      case _ => Tuple.fromSeq(dts) //this will prevent duplicates, TODO: eventually
    }
}

//---- Scalar ----//

class Scalar private[model2] (
         val id: Identifier,
         val valueType: ValueType,
         //val units: Option[String] = None, //TODO: MeasurementScale? still need to support string for now
         val missingValue: Option[Data] = None,
         val fillValue: Option[Data] = None,
         val precision: Option[Int] = None,
         val ascending: Boolean = true,
         val otherProperties: Map[String, String] = Map.empty //or keep all for easy to string?
       ) extends DataType {
  //TODO: coverage, cadence, resolution, start, end,... (see fdml schema)

  override def toString: String = id.asString
}

object Scalar extends ScalarFactory

//---- Tuple ----//

class Tuple private[model2] (val id: Option[Identifier], e1: DataType, e2: DataType, es: DataType*) extends DataType {

  // Guaranteed to have at least two elements //TODO: at least use NonEmptyList?
  def elements: List[DataType] = e1 +: (e2 +: es.toList)

  /** Returns a list of Tuple elements with no nested Tuples. */
  def flatElements: List[DataType] = elements.flatMap {
    case Tuple(es @ _*) => es
    case dt => List(dt)
  }

  override def toString: String = id.map(_.asString +": ").getOrElse("") + elements.mkString("(", ", ", ")")
}

object Tuple extends TupleFactory {
  def unapplySeq(tuple: Tuple): Some[Seq[DataType]] = Some(tuple.elements)
  //TODO: case Tuple(e1, e2, es @ _*) not considered exhaustive
}

//---- Function ----//

class Function private[model2] (val id: Option[Identifier], val domain: DataType, val range: DataType) extends DataType {

  override def toString: String = id.map(_.asString +": ").getOrElse("") + s"$domain -> $range"
  /*
  TODO: parens: f: (x -> a)?
    consider long form with scalar types: x: int -> a: int
      only know x is not a function because "int" is reserved?
   */
}

object Function extends FunctionFactory {
  def unapply(f: Function): Some[(DataType, DataType)] = Some((f.domain, f.range))
}


/*
case classes?
  can't extend
  no need to pattern match to extract scalar properties
  copy might be handy, but still needs validation

Real?
  require units, support precision, ...

construct with rich VariableMetadata? maybe later

InvalidStructure extends LatisException?

Serialize?

def metadata: Metadata to reconstruct Metadata (including class)
  e.g. Rename operation needed to ensure uniqueness but need to be able to remake Time
 */

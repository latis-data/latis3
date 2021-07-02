package latis.model2

import cats.syntax.all._

import latis.data.Data
import latis.data.Datum
import latis.data.DomainPosition
import latis.data.NullData
import latis.data.RangePosition
import latis.data.SamplePath
import latis.data.SamplePosition
import latis.metadata.Metadata
import latis.model.Index
import latis.model.ValueType
import latis.util.Identifier
import latis.util.LatisException

sealed trait DataType {
  //TODO: DataTypeOps?
  //def id: Option[Identifier]?
  //  only Projection, Rename look for id for any type, will need attention with qid
  //def scalars: List[Scalar] = ??? //needed? do we just need qualified ids for each scalar?
  //  generally used to zip with sample values
  //  nonIndexScalars?
  //  often a crutch without consideration for Functions in Tuples
  //  TODO: use this as an opportunity to sanity check assumptions about tuples and functions in tuples?
  //def flatten: DataType?
  //  used on Tuple only: GroupByVariable, TimeTupleToTime
  //    but nested tuples not usually considered
  //  and DataType.getPath
  //  what about nested tuples in nested functions? probably not
  //  note that we ofter use getScalars to avoid nesting issues
  //  use Tuple.flatElements
  //def rename or withId
  //  Pivot, Rename, DataType.flatten
  //  Note need copy constructor that subclasses can override so we preserve subclass
  //  TODO: need to enforce uniqueness but don't have scope
  //    smells like job for operation
  //    do we need to reconstruct from metadata with class?
  //    private to operation?
  //arity? vs dimensionality? (#56)
  //  DatasetGenerator, Curry, Evaluation, Resample
  //  note getScalars makes arity useless for nested tuples
  /*
  map?
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
    special caes don't need general solution here, but might be handy abstraction that opens doors?
   */
  //fold?

  /** Makes fill data for this data type. */
  def fillData: Data = {
    // Recursive helper function
    def go(dt: DataType, acc: Seq[Data]): Seq[Data] = dt match {
      case s: Scalar      => acc :+ s.fillValue.orElse(s.missingValue).getOrElse(NullData)
      case Tuple(es @ _*) => es.flatMap(go(_, acc))
      case _: Function    => acc :+ NullData
    }
    Data.fromSeq(go(this, Seq.empty))
  }

  def fold[A](
    fs: Scalar   => A,
    ft: Tuple    => A,
    ff: Function => A
  ): A = this match {
    case s: Scalar   => fs(s)
    case t: Tuple    => ft(t)
    case f: Function => ff(f)
  }

  def findVariable2(id: Identifier): Option[DataType] = {
    def fun(dt: DataType): Option[DataType] = dt.fold(
      (s: Scalar) =>
        Option.when(s.id == id)(s),
      (t: Tuple)  =>
        if (t.id.contains(id)) t.some
        else t.elements.map(fun).collectFirst {case Some(dt) => dt},
      (f: Function) =>
        if (f.id.contains(id)) f.some
        else fun(f.domain).orElse(fun(f.range))
    )
    fun(this)
  }

  /** Finds the first variable with the given Identifier. */
  //TODO: deal with QID
  def findVariable(id: Identifier): Option[DataType] = this match {
    case s: Scalar =>
      Option.when(s.id == id)(s)
    case t @ Tuple(es @ _*) =>
      if (t.id.contains(id)) t.some
      else es.find(_.findVariable(id).nonEmpty)
    case f @ Function(d, r) =>
      if (f.id.contains(id)) f.some
      else d.findVariable(id).orElse(r.findVariable(id))
  }

  /** Gets the path to the given variable in a Sample. */
  //TODO: deal with QID
  def getPath(id: Identifier): Option[SamplePath] = {
    // Helper function to make SamplePosition
    def position(domain: Boolean, index: Int): SamplePosition =
      if (domain) DomainPosition(index)
      else RangePosition(index)

//    def go(dt: DataType, path: SamplePath, domain: Boolean, index: Int): Option[SamplePath] = dt match {
//      case s: Scalar =>
//        //TODO: exclude Index
//        Option.when(s.id == id)(position(domain, index))
////        if (s.id == id) path :+ position(domain, index) //TODO: not our job to add to list?
////        else path
//
//      case t: Tuple => t.flatElements.filterNot(_.isInstanceOf[Index]).zipWithIndex.map {
//        case (dt, i) => go(dt, path, domain, i)
//        ???
//      }
//
//      case Function(d, r) => ???
//
//    }
//
//    // Note: domain=false and index=0 is the SamplePosition for a constant.
//    go(this, List.empty, false, 0) //TODO: Some or None if empty
    /*
    TODO: consider fold: https://files.slack.com/files-pri/T3JCZ2S87-F026Z303J8G/fun_with_qualified_identifiers.scala
     */
    ???
  }
}

object DataType {

  // Deal with Tuple arity < 2 complications
  def fromSeq(dts: Seq[DataType]): Either[LatisException, DataType] =
    dts.length match {
      case 0 => LatisException("Empty DataType not supported").asLeft
      case 1 => dts.head.asRight
      case _ => Tuple.fromSeq(dts) //this will prevent duplicates, TODO: eventually
    }

  //private def idsUnique(dts: Seq[DataType]): Boolean = ???
}

//---- Scalar ----//

class Scalar private (
         val id: Identifier,
         val valueType: ValueType,
         //val units: Option[String] = None, //TODO: MeasurementScale? still need to support string for now
         val missingValue: Option[Data] = None,
         val fillValue: Option[Data] = None,
         //val precision: Option[Int] = None,
         val otherProperties: Map[String, String] = Map.empty //or keep all for easy to string?
       ) extends DataType {
  //TODO: coverage, cadence, resolution, start, end,... (see fdml schema)

  //TODO: ScalarOps to keep noise out of sealed trait?
  def isFillable: Boolean = fillValue.nonEmpty
  def parseValue(value: String): Either[LatisException, Datum] = ???

  override def toString: String = id.asString
}

object Scalar {

  def apply(id: Identifier, valueType: ValueType): Scalar = new Scalar(id, valueType)

  //TODO: extend ScalarFactory? keep noise out of sealed trait
  //TODO!: construct subclass for "class"
  def fromMetadata(metadata: Metadata): Either[LatisException, Scalar] = for {
    id <- metadata.getProperty("id")
      .toRight(LatisException("No id defined"))
      .flatMap(Identifier.fromString(_).toRight(LatisException("Invalid Identifier")))
    valueType <- metadata.getProperty("type")
      .toRight(LatisException("No type defined"))
      .flatMap(ValueType.fromName)
    missingValue <- metadata.getProperty("missingValue").traverse(valueType.parseValue)
    fillValue <- metadata.getProperty("fillValue").traverse(valueType.parseValue)
    other = metadata.properties.filterNot(p => List("id", "type", "missingValue", "fillValue").contains(p._1))
  } yield new Scalar(
    id,
    valueType,
    missingValue,
    fillValue,
    otherProperties = other
  ) //TODO: other properties

}

//---- Tuple ----//

class Tuple private (val id: Option[Identifier], e1: DataType, e2: DataType, es: DataType*) extends DataType {

  // Guaranteed to have at least two elements //TODO: at least use NonEmptyList?
  def elements: List[DataType] = e1 +: (e2 +: es.toList)

  /** Returns a list of Tuple elements with no nested Tuples. */
  def flatElements: List[DataType] = elements.flatMap {
    case Tuple(es @ _*) => es
    case dt => List(dt)
  }

  override def toString: String = id.map(_.asString +": ").getOrElse("") + elements.mkString("(", ", ", ")")
}

object Tuple {

  def fromElements(id: Option[Identifier], e1: DataType, e2: DataType, es: DataType*): Either[LatisException, Tuple] =
  //TODO: assert unique ids
    new Tuple(id, e1, e2, es: _*).asRight

  def fromElements(id: Identifier, e1: DataType, e2: DataType, es: DataType*): Either[LatisException, Tuple] =
    fromElements(Some(id), e1, e2, es: _*)

  def fromElements(e1: DataType, e2: DataType, es: DataType*): Either[LatisException, Tuple] =
    fromElements(None, e1, e2, es: _*)

  def fromSeq(id: Option[Identifier], dts: Seq[DataType]): Either[LatisException, Tuple] =
    dts match {
      case e1 :: e2 :: es => fromElements(id, e1, e2, es: _*)
      case _ => LatisException("Tuple must have at least two elements").asLeft
    }

  def fromSeq(id: Identifier, dts: Seq[DataType]): Either[LatisException, Tuple] = fromSeq(Some(id), dts)

  def fromSeq(dts: Seq[DataType]): Either[LatisException, Tuple] = fromSeq(None, dts)

  def unapplySeq(tuple: Tuple): Some[Seq[DataType]] = Some(tuple.elements)
  //TODO: case Tuple(e1, e2, es @ _*) not considered exhaustive
}

//---- Function ----//

class Function private (val id: Option[Identifier], val domain: DataType, val range: DataType) extends DataType {

  override def toString: String = id.map(_.asString +": ").getOrElse("") + s"$domain -> $range"
}

object Function {

  def from(id: Option[Identifier], domain: DataType, range: DataType): Either[LatisException, Function] = {
    //TODO: assert unique ids
    //TODO: assert no functions in domain, including within tuples
    new Function(id, domain, range).asRight
  }

  def from(id: Identifier, domain: DataType, range: DataType): Either[LatisException, Function] =
    from(Some(id), domain, range)

  def from(domain: DataType, range: DataType): Either[LatisException, Function] =
    from(None, domain, range)

  def unapply(f: Function): Some[(DataType, DataType)] = Some(f.domain, f.range)
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

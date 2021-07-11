package latis.model

import cats.syntax.all._

import latis.data._
import latis.util.Identifier

trait DataTypeAlgebra { dataType: DataType =>

  /** Recursively apply a function to this DataType. */
  /*
  TODO: review dangers and potential misuse
   Current usage:
   ConvertTime.scala:56
    Time => Time
   FormatTime.scala:57
    Time => Time
   Rename.scala:20
    any DT to same type
   TimeTupleToTime.scala:21
    Tuple => Time
   not generally safe for any type mapping, changes structure that we are traversing over
   limit to Scalar => Scalar? TimeTextToTime is on its own?
   substitution use case? uri => Function
   special cases don't need general solution here, but might be handy abstraction that opens doors?
 */
  //@deprecated("Not safe if the type changes?")
  def map(f: DataType => DataType): DataType = dataType match {
    case s: Scalar => f(s)
    case t @ Tuple(es @ _*) => f(Tuple.fromSeq(t.id, es.map(f)).fold(throw _, identity))
    case func @ Function(d, r) => f(Function.from(func.id, d.map(f), r.map(f)).fold(throw _, identity))
  }

  /**
   * Returns a List of Scalars in the (depth-first) order
   * that they appear in the model.
   */
  //TODO: deprecate to find potential unsafe
  //  Risk of overlooking tuples and functions nested in tuples
  //  Risk of being inconsistent with arity vs dimensionality
  // use "scalars", "nonIndexScalars" to migrate from getScalars?
  def getScalars: List[Scalar] = {
    def go(dt: DataType): List[Scalar] = dt match {
      case s: Scalar      => List(s)
      case t: Tuple       => t.elements.flatMap(go)
      case Function(d, r) => go(d) ++ go(r)
    }
    go(dataType)
  }

  /** Replaces the identifier of this DataType. */
  def rename(id: Identifier): DataType = dataType match {
    case s: Scalar      =>
      // Note that this will preserve subclass via "class" metadata
      Scalar.fromMetadata(s.metadata + ("id" -> id.asString)).fold(throw _, identity)
    case t: Tuple       => Tuple.fromSeq(id, t.elements).fold(throw _, identity)
    case Function(d, r) => Function.from(id, d, r).fold(throw _, identity)
  }

  /**
   * Returns the arity of this DataType.
   *
   * For a Function, this is the number of top level types (non-flattened)
   * in the domain. For Scalar and Tuple, there is no domain so the arity is 0.
   *
   * Beware that this is not the same as dimensionality since a nested tuple
   * counts as one towards arity.
   */
  def arity: Int = dataType match {
    case Function(domain, _) =>
      domain match {
        case _: Scalar   => 1
        case t: Tuple    => t.elements.length
        case _: Function => ??? //bug, Function not allowed in domain
      }
    case _ => 0
  }

  /** Makes fill data for this data type. */
  def fillData: Data = {
    // Recursive helper function
    def go(dt: DataType, acc: Seq[Data]): Seq[Data] = dt match {
      case s: Scalar      => acc :+ s.fillValue.orElse(s.missingValue).getOrElse(NullData)
      case Tuple(es @ _*) => es.flatMap(go(_, acc))
      case _: Function    => acc :+ NullData
    }
    Data.fromSeq(go(dataType, Seq.empty))
  }

  /** Finds the first variable with the given Identifier. */
  def findVariable(id: Identifier): Option[DataType] = dataType match {
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
  def findPath(id: Identifier): Option[SamplePath] =
    PathFinder.findPath(this, id)

}

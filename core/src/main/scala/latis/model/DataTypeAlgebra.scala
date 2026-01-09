package latis.model

import scala.annotation.tailrec

import cats.syntax.all.*

import latis.data.*
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
    case t: Tuple  => f(Tuple.fromSeq(t.id, t.elements.map(f)).fold(throw _, identity))
    case func @ Function(d, r) => f(Function.from(func.id, d.map(f), r.map(f)).fold(throw _, identity))
  }

  /**
   * Returns a List of Scalars in the (depth-first) order
   * that they appear in the model.
   */
  def getScalars: List[Scalar] = {
    def go(dt: DataType): List[Scalar] = dt match {
      case s: Scalar      => List(s)
      case t: Tuple       => t.elements.flatMap(go)
      case Function(d, r) => go(d) ++ go(r)
    }
    go(dataType)
  }
  
  /** Returns Scalars in the model that are not an Index. */
  def nonIndexScalars: List[Scalar] =
    getScalars.filterNot(_.isInstanceOf[Index])

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

  /**
   * Does this DataType have simply nested Functions.
   *
   * A nested Function that exist as the sole range variable of another
   * Function is considered simply nested. This applies to all levels
   * of nesting. This property constrains the types of Operations and
   * Encoders that can be applied. Such Datasets can be flattened with
   * the Uncurry operation. Note that this is unrelated to nested Tuples.
   */
  def isSimplyNested: Boolean = dataType match {
    case Function(_, f: Function) => ! f.isComplex
    case _ => false
  }

  /**
   * Does this DataType have nested Functions that are not simply nested.
   *
   * A Tuple that contains a Function, including a Tuple in the range of
   * a Function, is considered to be complex. Many Operations and Encoders
   * cannot directly be applied without some sort of join.
   */
  def isComplex: Boolean = {
    @tailrec def go(model: DataType): Boolean = model match {
      case Function(_, dt) => go(dt)
      case t: Tuple        => t.flatElements.exists(_.isInstanceOf[Function])
      case _               => false
    }
    go(this)
  }


  /** Makes fill data for this data type. */
  def fillData: Data = {
    // Recursive helper function
    def go(dt: DataType, acc: Seq[Data]): Seq[Data] = dt match {
      case s: Scalar      => acc :+ s.fillValue.orElse(s.missingValue).getOrElse(NullData)
      case t: Tuple       => t.elements.flatMap(go(_, acc))
      case _: Function    => acc :+ NullData
    }
    Data.fromSeq(go(dataType, Seq.empty))
  }

  /** Finds the first variable with the given Identifier. */
  def findVariable(id: Identifier): Option[DataType] = dataType match {
    case s: Scalar =>
      Option.when(s.id == id)(s)
    case t: Tuple =>
      if (t.id.contains(id)) t.some
      else t.elements.find(_.findVariable(id).nonEmpty)
    case f @ Function(d, r) =>
      if (f.id.contains(id)) f.some
      else d.findVariable(id).orElse(r.findVariable(id))
  }

  /** Gets the path to the given variable in a Sample. */
  def findPath(id: Identifier): Option[SamplePath] =
    PathFinder.findPath(this, id)

  /** Tests whether the predicate holds for at least one element of the model. */
  def exists(p: DataType => Boolean): Boolean = {
    def go(model: DataType): Boolean = {
      if (p(model)) true else model match {
        case _: Scalar      => false
        case t: Tuple       => t.elements.exists(go)
        case Function(d, r) => go(d) || go(r)
      }
    }
    go(this)
  }
}

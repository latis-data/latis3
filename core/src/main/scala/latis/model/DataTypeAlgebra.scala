package latis.model

import cats.syntax.all._

import latis.data._
import latis.util.Identifier

trait DataTypeAlgebra { dataType: DataType =>

  /** Recursively apply a function to this DataType. */
  //TODO: review dangeers and potential misuse,
  // make a safer option?
  // Scalar => Scalar? but TimeTuple => Time?
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
  def getPath(id: Identifier): Option[SamplePath] = {
    // Helper function to make SamplePosition.
    // tdr: "t"op, "d"omain, or "r"ange
    def position(tdr: String, index: Int): SamplePosition = tdr match {
      case "t" => RangePosition(index)
      case "d" => DomainPosition(index)
      case "r" => RangePosition(index)
      //TODO: nowarn annotation
    }

    // Build list of id, path pairs
    def accumulateIdPathPairs(
      dt: DataType,
      path: SamplePath,
      tdr: String, // top, domain, or range
      index: Int
    ): List[(Identifier, SamplePath)] = dt match {
      case _: Index  => List.empty //Index not represented in Sample
      case s: Scalar => List((s.id, path :+ position(tdr, index)))
      case t: Tuple  =>
        //TODO: path to tuple should be path to first element
        //  so we can't simply flatten named nested tuples away
        // --TupleData is not allowed in a Sample, so no path for a Tuple.--
        // Top level Tuple shows up in Sample range.
        val newTdr = if (tdr == "t") "r" else tdr
        // Samples do not preserve tuple nesting.
        t.flatElements.zipWithIndex.flatMap { case (e, i) =>
          accumulateIdPathPairs(e, path, newTdr, i)
        }
      case f: Function =>
        // Top level function has no position in a Sample.
        val newPath = if (tdr == "t") List.empty else path :+ position(tdr, index)
        f.id.map((_, newPath)).toList ++ //empty if id not defined
          accumulateIdPathPairs(f.domain, newPath, "d", 0) ++
          accumulateIdPathPairs(f.range,  newPath, "r", 0)
    }

    // Find the path for the given id
    //TODO: short circuit
    accumulateIdPathPairs(dataType, List.empty, "t", 0).find(_._1 == id).map(_._2)
  }
}

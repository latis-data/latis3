package latis.model2

import cats.syntax.all._

import latis.data._
import latis.util.Identifier

class DataTypeOps(dataType: DataType) {

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
        // TupleData is not allowed in a Sample, so no path for a Tuple.
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

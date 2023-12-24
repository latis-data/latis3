package latis.model

import latis.data.*
import latis.util.Identifier

object PathFinder {

  sealed trait TopDomainOrRange
  object Top extends TopDomainOrRange
  object Domain extends TopDomainOrRange
  object Range extends TopDomainOrRange

  private def position(tdr: TopDomainOrRange, index: Int): SamplePosition = tdr match {
    case Top    => RangePosition(index)
    case Domain => DomainPosition(index)
    case Range  => RangePosition(index)
  }

  private def accumulateTypePathPairs(
    dt: DataType,
    path: SamplePath,
    tdr: TopDomainOrRange,
    index: Int
  ): List[(DataType, SamplePath)] = dt match {
    case _: Index  => List.empty //Index not represented in Sample
    case s: Scalar => List((s, path :+ position(tdr, index)))
    case t: Tuple  =>
      val newTdr = if (tdr == Top) Range else tdr
      List((t, path :+ position(tdr, index))) ++ { //this Tuple if named
        var i = index //track position within this tuple
        t.elements.flatMap { e =>
          val pairs = accumulateTypePathPairs(e, path, newTdr, i) //recurse on elements
          // Increment position accounting for nested content
          //   Don't count nested Tuple itself
          //   Don't count content of Function
          i = i + pairs.collect {
            case (s: Scalar, p) if (p.length == path.length + 1) => s
            case (f: Function, p) if (p.length == path.length + 1) => f
          }.length
          pairs
        }
      }
    case f: Function =>
      val newPath =
        if (tdr == Top) List.empty  //top level function has no position in a Sample
        else path :+ position(tdr, index)
      List((f, newPath)) ++                                      //this Function
        accumulateTypePathPairs(f.domain, newPath, Domain, 0) ++ //recurse on domain
        accumulateTypePathPairs(f.range,  newPath, Range, 0)     //recurse on range
  }

  def findPath(model: DataType, id: Identifier): Option[SamplePath] = {
    accumulateTypePathPairs(model, List.empty, Top, 0).find {
      case (s: Scalar, _)   => s.id == id
      case (t: Tuple, _)    => t.id.contains(id)
      case (f: Function, _) => f.id.contains(id)
    }.map(_._2)
  }
}

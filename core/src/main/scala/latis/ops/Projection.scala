package latis.ops

import cats.syntax.all.*

import latis.data.*
import latis.model.*
import latis.util.Identifier
import latis.util.LatisException

/**
 * Operation to project only a given set of variables in a Dataset.
 * Domain variables will be included, for now.
 */
case class Projection(ids: Identifier*) extends MapOperation {
  //TODO: support named tuples and functions
  //TODO: support nested Functions
  //TODO: support aliases, hasName
  //TODO: support dot notation for nested tuples
  //TODO: if Cartesian keep separate Index variable for each dimension
  //TODO: support partial domain projection:
  // if not Cartesian, move vars to range and replace domain with Index
  //TODO: reorder ids to be consistent with model (after we have model validation...)

  override def applyToModel(model: DataType): Either[LatisException, DataType] =
    applyToVariable(model).toRight(LatisException("Nothing projected"))

  /** Recursive method to apply the projection. */
  private def applyToVariable(v: DataType): Option[DataType] = v match {
    case s: Scalar =>
      if (ids.contains(s.id)) Some(s) else None
    case t: Tuple =>
      val vs = t.elements.flatMap(applyToVariable)
      vs.length match {
        case 0 => None // drop empty Tuple
        case 1 => Some(vs.head) // reduce Tuple of one
        case _ => Some(Tuple.fromSeq(vs).fold(throw _, identity))
      }
    case Function(domain, range) =>
      // Behavior depends on whether domain variables are projected
      //TODO: if Cartesian, replace each non-projected domain scalar with index
      applyToVariable(domain) match {
        // Domain unchanged
        case Some(d) if (d == domain) =>
          applyToVariable(range) match {
            case Some(r) => Some(Function.from(domain, r).fold(throw _, identity))
            // If no range variables projected, put domain in range and make index domain
            case None => Some(Function.from(makeIndex(domain), domain).fold(throw _, identity))
          }
        // Not all domain variables projected.
        case Some(_) =>
          //TODO: Move remaining vars to range and replace domain with single index.
          throw LatisException("Partial domain projection not yet supported.")
        // No domain variable projected
        case None =>
          // Replace domain with Index
          // None if no variables projected
          applyToVariable(range).map { r =>
            Function.from(makeIndex(domain), r).fold(throw _, identity)
          }
      }
  }

  override def mapFunction(model: DataType): Sample => Sample = {
    // Get the sample positions of the projected variables.
    // Assumes Scalar projection only, for now.
    // Does not support nested Function, for now.
    val positions = model
      .getScalars                //start here so we preserve variable order
      .map(_.id)
      .filter(ids.contains(_))   //keep the projected ids
      .map(model.findPath(_).get) //findPath should be safe since we just got the id from the model
      .map {
        case p :: Nil => p
        case _ :: _   => throw LatisException("Can't project within nested Function.")
        case _        => ??? //shouldn't happen
      }
    // Separate domain and range positions
    val (dpos, rpos): (List[DomainPosition], List[RangePosition]) =
      positions.foldLeft((List[DomainPosition](), List[RangePosition]())) {
        (p, pos) => pos match {
          case dp: DomainPosition => ((p._1 :+ dp), p._2)
          case rp: RangePosition => (p._1, (p._2 :+ rp))
        }
      }

    val domainIndices: List[Int] = dpos.map {
      case DomainPosition(i) => i
    }
    val rangeIndices: List[Int] = rpos.map {
      case RangePosition(i) => i
    }

    if (rangeIndices.isEmpty) {
      (sample: Sample) =>
        Sample(DomainData(), domainIndices.map(sample.domain))
    } else {
      (sample: Sample) =>
        Sample(domainIndices.map(sample.domain), rangeIndices.map(sample.range))
    }
  }

  /**
   * Makes an Index to replace a variable.
   * This is used when a domain variable needs a placeholder
   * when it is not projected. The identifier for the Index
   * will be the original identifier prepended with "_i".
   * If the variable does not have an id (e.g. anonymous tuple),
   * a random unique id will be generated.
   * Note that no other metadata is preserved.
   */
  private def makeIndex(v: DataType): Index = v match {
    case i: Index  => i //no-op if already an Index
    case s: Scalar =>
      Index(Identifier.fromString("_i" + s.id.asString).get) //safely valid
    case t: Tuple  => t.id.map { id =>
      Index(Identifier.fromString("_i" + id.asString).get) //safely valid
    }.getOrElse {
      // Derive id by combining scalar ids with "_"
      Index(Identifier.fromString("_i" + v.getScalars.map(_.id.asString).mkString("_")).get)
    }
    case _: Function => ??? //Function not allowed in domain
  }

}

object Projection {

  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromExpression(exp: String): Either[LatisException, Projection] =
    fromArgs(exp.split("""\s*,\s*""").toList)

  def fromArgs(args: List[String]): Either[LatisException, Projection] = args match {
    case Nil => Left(LatisException("Projection requires at least one argument"))
    case _ => args.traverse { id =>
      Identifier.fromString(id).toRight(LatisException(s"'$id' is not a valid identifier"))
    }.map(Projection(_ *))
  }

}

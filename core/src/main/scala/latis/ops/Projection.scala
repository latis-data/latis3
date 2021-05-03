package latis.ops

import cats.syntax.all._

import latis.data._
import latis.model._
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

  override def applyToModel(model: DataType): Either[LatisException, DataType] =
    applyToVariable(model).toRight(LatisException("Nothing projected"))

  /** Recursive method to apply the projection. */
  private def applyToVariable(v: DataType): Option[DataType] = v match {
    case s: Scalar =>
      if (s.id.exists(id => ids.contains(id))) Some(s) else None
    case Tuple(vars @ _*) =>
      val vs = vars.flatMap(applyToVariable)
      vs.length match {
        case 0 => None // drop empty Tuple
        case 1 => Some(vs.head) // reduce Tuple of one
        case _ => Some(Tuple(vs))
      }
    case Function(domain, range) =>
      // Behavior depends on whether domain variables are projected
      //TODO: if Cartesian, replace each non-projected domain scalar with index
      applyToVariable(domain) match {
        // Domain unchanged
        case Some(d) if (d == domain) =>
          applyToVariable(range) match {
            case Some(r) => Some(Function(domain, r))
            // If no range variables projected, put domain in range and make index domain
            case None => Some(Function(makeIndex(domain), domain))
          }
        // Not all domain variables projected.
        case Some(d) =>
          //TODO: Move remaining vars to range and replace domain with single index.
          throw LatisException("Partial domain projection not yet supported.")
        // No domain variable projected
        case None =>
          // Replace domain with Index
          // None if no variables projected
          applyToVariable(range).map { r =>
            Function(makeIndex(domain), r)
          }
      }
  }

  override def mapFunction(model: DataType): Sample => Sample = {
    // Get the sample positions of the projected variables.
    // Assumes Scalar projection only, for now.
    // Does not support nested Function, for now.
    val (dpos, rpos): (List[SamplePosition], List[SamplePosition]) = model
      .getScalars                //start here so we preserve variable order
      .map(_.id.get)
      .filter(ids.contains(_))   //keep the projected ids
      .map(model.getPath(_).get) //getPath should be safe since we just got the id from the model
      .map {
        case p :: Nil => p
        case _ :: _   => throw LatisException("Can't project within nested Function.")
        case _        => ??? //shouldn't happen
      }.partition(_.isInstanceOf[DomainPosition])

    val domainIndices: List[Int] = dpos.map {
      case DomainPosition(i) => i
    }
    val rangeIndices: List[Int] = rpos.map {
      case RangePosition(i) => i
    }

    if (rangeIndices.isEmpty) {
      (sample: Sample) => sample match {
        case Sample(ds, rs) => Sample(DomainData(), domainIndices.map(ds))
      }
    } else {
      (sample: Sample) => sample match {
        case Sample(ds, rs) => Sample(domainIndices.map(ds), rangeIndices.map(rs))
      }
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
    case i: Index => i //no-op if already an Index
    case _ => v.id.map { id =>
      Index("_i" + id.asString)
    }.getOrElse {
      // Derive Id
      Index("_i" + v.getScalars.map(_.id.get.asString).mkString("_"))
    }
  }
}

object Projection {

  def fromExpression(exp: String): Either[LatisException, Projection] =
    fromArgs(exp.split("""\s*,\s*""").toList)

  def fromArgs(args: List[String]): Either[LatisException, Projection] = args match {
    case Nil => Left(LatisException("Projection requires at least one argument"))
    case _ => args.traverse { id =>
      Identifier.fromString(id).toRight(LatisException(s"'$id' is not a valid identifier"))
    }.map(Projection(_: _*))
  }

}

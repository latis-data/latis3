package latis.ops

import cats.data.ValidatedNel
import cats.syntax.all.*

import latis.data.*
import latis.model.*
import latis.util.Identifier
import latis.util.LatisException

/**
 * Operation to include only a given set of variables in a Dataset.
 *
 * The order of the projected ids is ignored (unlike SQL) to ensure
 * the integrity of the model (e.g. preserve domain).
 * All given variables (by id) must appear in the top level of the
 * model, not in nested Functions unless all variables are projected.
 * If the domain variable(s) (all or none, for now) are not projected,
 * they will be replaced by an Index placeholder variable. If only the
 * domain is projected, it will become the range with the domain
 * replaced by an Index. If all variables are projected, the processing
 * will be skipped.
 *
 * @params ids Identifiers of the Scalar variable to keep
 */
case class Projection(ids: Identifier*) extends MapOperation {
  //TODO: use NonEmpty, rely on validator for now
  //  or Set since order does not matter?
  //  or should we allow an empty projection, Monoid, need NullDataType?
  //TODO: support named tuples and functions
  //TODO: support dot notation for elements of nested tuples
  //TODO: support nested Functions
  //TODO: if Cartesian, keep separate Index variable for each dimension
  //TODO: support partial domain projection:
  //  if Cartesian, replace non-projected variables with Index
  //  if not Cartesian, move vars to range and replace domain with Index
  //    otherwise lose ordering

  /** Apply projection to model */
  override def applyToModel(model: DataType): Either[LatisException, DataType] = {
    validate(model)
      .leftMap(es => es.head).toEither // Only one Exception
      .flatMap { proj =>
        if (allProjected(model)) model.asRight //no-op
        else proj.applyToVariable(model)
          .toRight(LatisException("Bug: Nothing projected, shouldn't have validated"))
      }
  }

  /** Recursive method to apply the projection to a variable type. */
  private def applyToVariable(v: DataType): Option[DataType] = v match {
    case s: Scalar =>
      if (ids.contains(s.id)) Some(s) else None
    case t: Tuple =>
      val vs = t.elements.flatMap(applyToVariable)
      vs.length match {
        case 0 => None          // drop empty Tuple
        case 1 => Some(vs.head) // reduce Tuple of one
        case _ => Some(Tuple.fromSeq(vs).fold(throw _, identity))
      }
    case Function(domain, range) =>
      // Behavior depends on whether domain variables are projected
      applyToVariable(domain) match {
        // Domain unchanged
        case Some(d) if (equivalentScalarIds(d, domain)) =>
          applyToVariable(range) match {
            case Some(r) => Some(Function.from(domain, r).fold(throw _, identity))
            // If no range variables projected, put domain in range and make index domain
            case None => Some(Function.from(makeIndex(domain), domain).fold(throw _, identity))
          }
        // Not all domain variables projected.
        case Some(_) =>
          throw LatisException("Partial domain projection not yet supported.")
        // No domain variable projected
        case None =>
          // Replace domain with Index
          applyToVariable(range).map { r =>
            Function.from(makeIndex(domain), r).fold(throw _, identity)
          }
      }
  }

  /**
   * Determines if two variables are effectively equivalent in the context of
   * projections by comparing the contained Scalar variable ids.
   */
  private def equivalentScalarIds(v1: DataType, v2: DataType): Boolean = {
    val vs1 = v1.getScalars
    val vs2 = v2.getScalars
    (vs1.length == vs2.length) && vs1.zip(vs2).forall(p => p._1.id == p._2.id)
  }

  /** Apply projection to data */
  override def mapFunction(model: DataType): Sample => Sample = {
    // Bail early if the projection is not valid
    // Hopefully caught at model application, but...
    if (validate(model).isInvalid) throw LatisException("Bad projection")

    if (allProjected(model)) identity //no-op
    else {
      // Get the sample positions of the projected variables.
      // Assumes Scalar projection only, for now.
      // Does not support nested Function, for now.
      val positions = model
        .nonIndexScalars //start here so we preserve variable order
        .map(_.id)
        .filter(ids.contains)
        .map(model.findPath(_).get) //safe since id is from the model
        .map {
          case p :: Nil => p
          case _ => throw LatisException("Bug: Projection validation failed")
        }

      // Separate domain and range position indices
      val (domainIndices, rangeIndices) = positions.partitionMap {
        case DomainPosition(i) => i.asLeft
        case RangePosition(i) => i.asRight
      }

      if (rangeIndices.isEmpty) {
        (sample: Sample) =>
          Sample(DomainData(), domainIndices.map(sample.domain))
      } else {
        (sample: Sample) =>
          Sample(domainIndices.map(sample.domain), rangeIndices.map(sample.range))
      }
    }
  }

  /**
   * Makes an Index to replace a non-projected domain variable.
   *
   * This is used when a domain variable needs a placeholder
   * when it is not projected. The identifier for the Index
   * will be the original identifier prepended with "_i".
   * If the variable does not have an id (e.g. anonymous tuple),
   * an id will be generated using its member Scalar ids.
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
      Index(Identifier.fromString("_i" + v.getScalars.map(_.id).mkString("_")).get)
    }
    case _: Function =>
      //Function not allowed in domain
      throw LatisException("Can't replace a Function with an Index")
  }

  /** Make sure this projection can be applied to the given model. */
  def validate(model: DataType): ValidatedNel[LatisException, Projection] = {
    // Some variables have been projected
    def nonEmpty =
      if (ids.isEmpty) LatisException("No variables projected").invalidNel
      else this.valid

    // All projected variables are non-Index Scalars in the model
    def variablesDefined = {
      val scalarIds = model.nonIndexScalars.map(_.id)
      ids.filter(id => !scalarIds.contains(id)) match {
        case Nil => this.valid
        case id :: Nil =>
          val msg = s"Projected Scalar does not exist: $id"
          LatisException(msg).invalidNel
        case ids =>
          val badIds = ids.mkString(", ")
          val msg = s"Projected Scalars do not exist: $badIds"
          LatisException(msg).invalidNel
      }
    }

    // No projected variables are in a nested Function unless all are projected
    def noNestingUnlessAll = {
      if (allProjected(model)) this.valid
      else if (ids.forall(model.findPath(_).get.size == 1)) this.valid
      else {
        val msg = "Projected variables may not be in nested Functions"
        LatisException(msg).invalidNel
      }
    }

    nonEmpty.andThen(_ => variablesDefined).andThen(_ => noNestingUnlessAll)
  }

  /** Are all variables in the dataset projected */
  private def allProjected(model: DataType): Boolean =
    model.getScalars.forall(s => ids.contains(s.id))

  override def toString = s"Projection(${ids.mkString(",")})"
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

package latis.util

import contextual._
import latis.util.IdentifierInterpolator.checkValidIdentifier

sealed abstract case class Identifier(asString: String)
object Identifier {
  def fromString(id: String): Option[Identifier] =
    if (checkValidIdentifier(id)) Some(new Identifier(id) {}) else None
  
  implicit class IdentifierStringContext(val sc: StringContext) extends AnyVal {
    def id = Prefix(IdentifierInterpolator, sc)
  }
}

/**
 * Defines a String interpolator that will raise a compile error if the String isn't a valid LaTiS identifier. 
 */
object IdentifierInterpolator extends Interpolator {
  
  type Output = Identifier

  def contextualize(interpolation: StaticInterpolation) = {
    val lit@Literal(_, idString) = interpolation.parts.head
    if(!checkValidIdentifier(idString))
      interpolation.abort(lit, 0, "not a valid LaTiS identifier")

    Nil
  }

  def evaluate(interpolation: RuntimeInterpolation): Identifier =
    Identifier.fromString(interpolation.literals.head).get
  
  /** Returns whether the String is a regex "word" that doesn't start with a digit. */
  def checkValidIdentifier(str: String): Boolean = str.matches("^(?!\\d)\\w+")
  
}

package latis.util

import contextual._

case class Identifier(id: String) extends AnyVal {
  override def toString: String = id
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
    Identifier(interpolation.literals.head)
  
  def checkValidIdentifier(str: String): Boolean = str.matches("\\w+")

  implicit class IdentifierStringContext(sc: StringContext) {
    val id = Prefix(IdentifierInterpolator, sc)
  }
  
}

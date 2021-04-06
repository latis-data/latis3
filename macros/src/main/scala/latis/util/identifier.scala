package latis.util

import scala.reflect.macros.blackbox.Context

/**
 * Defines a LaTiS identifier, a name that may contain only:
 *   - letters
 *   - numbers
 *   - underscores
 *   - dots (TODO: remove support for dots)
 * so long as the identifier does not start with a number.
 */
sealed abstract case class Identifier(asString: String)
object Identifier {
  def fromString(id: String): Option[Identifier] =
    if (checkValidIdentifier(id)) Some(new Identifier(id) {}) else None

  implicit class IdentifierStringContext(val sc: StringContext) extends AnyVal {
    def id(): Identifier = macro literalMacro
  }

  /** Returns whether the String is a regex "word" that doesn't start with a digit (may also contain dots). */
  private def checkValidIdentifier(str: String): Boolean =
    str.matches("^(?!\\d)(\\w|\\.)+") //TODO: revert to "^(?!\\d)\\w+"

  def literalMacro(c: Context)(): c.Expr[Identifier] = {
    import c.universe._

    val idExpr: Expr[String] = c.prefix.tree match {
      case Apply(_, Apply(_, List(id @ Literal(Constant(s: String)))) :: Nil) =>
        if (checkValidIdentifier(s)) c.Expr(id)
        else c.abort(c.enclosingPosition, "not a valid LaTiS identifier")
    }

    reify(Identifier.fromString(idExpr.splice).get)
  }
}

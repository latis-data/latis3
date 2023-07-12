package latis.util

import org.typelevel.literally.Literally

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
    if (isValidIdentifier(id)) Some(new Identifier(id) {}) else None

  implicit class IdentifierStringContext(val sc: StringContext) extends AnyVal {
    def id(args: Any*): Identifier = macro IdentifierLiteral.make
  }

  /**
   * Returns whether the String is a regex "word" that doesn't start
   * with a digit (may also contain dots).
   */
  private[util] def isValidIdentifier(str: String): Boolean =
    str.matches("^(?!\\d)(\\w|\\.)+") //TODO: revert to "^(?!\\d)\\w+"
}

object IdentifierLiteral extends Literally[Identifier] {
  override def validate(c: Context)(s: String): Either[String, c.Expr[Identifier]] = {
    import c.universe._
    Either.cond(
      Identifier.isValidIdentifier(s),
      c.Expr(q"_root_.latis.util.Identifier.fromString($s).get"),
      "not a valid LaTiS identifier"
    )
  }

  def make(c: Context)(args: c.Expr[Any]*): c.Expr[Identifier] = apply(c)(args: _*)
}

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

  extension(inline ctx: StringContext) {
    inline def id(inline args: Any*): Identifier =
      ${IdentifierLiteral('ctx, 'args)}
  }

  /**
   * Returns whether the String is a regex "word" that doesn't start
   * with a digit (may also contain dots).
   */
  private[util] def isValidIdentifier(str: String): Boolean =
    str.matches("^(?!\\d)(\\w|\\.)+") //TODO: revert to "^(?!\\d)\\w+"
}

object IdentifierLiteral extends Literally[Identifier] {
  override def validate(s: String)(using Quotes): Either[String, Expr[Identifier]] =
    Either.cond(
      Identifier.isValidIdentifier(s),
      '{Identifier.fromString(${Expr(s)}).get},
      "not a valid LaTiS identifier"
    )
}

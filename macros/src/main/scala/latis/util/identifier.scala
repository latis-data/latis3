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

opaque type Identifier = String

object Identifier {
  def fromString(id: String): Option[Identifier] =
    if (isValidIdentifier(id)) Option(id) else None

  extension (id: Identifier) {
    def asString: String = id
  }

  extension(inline ctx: StringContext) {
    inline def id(inline args: Any*): Identifier =
      ${IdentifierLiteral('ctx, 'args)}
  }

  given Ordering[Identifier] with
    def compare(x: Identifier, y: Identifier): Int = x.compareTo(y)

  /**
   * Returns whether the String is a regex "word" that doesn't start
   * with a digit (may also contain dots).
   */
  private[util] def isValidIdentifier(str: String): Boolean =
    str.matches("^(?!\\d)(\\w|\\.)+") //TODO: revert to "^(?!\\d)\\w+"
}

object IdentifierLiteral extends Literally[Identifier] {
  override def validate(s: String)(using Quotes): Either[String, Expr[Identifier]] = {
    val expr = Expr(s)
    Either.cond(
      Identifier.isValidIdentifier(s),
      '{Identifier.fromString($expr).get},
      "not a valid LaTiS identifier"
    )
  }
}

package latis.util

object StringUtil {

  private[this] val doubleQuote: Char = '"'
  private[this] val singleQuote: Char = '\''
  private[this] val backslash:   Char = '\\'

  /**
   * Ensures that the given string is surrounded by the given quote character.
   * Internal quotes will be escaped with a backslash (\). There will be no
   * change if the first and last characters are already quotes.
   */
  def ensureQuoted(string: String, quote: Char): String = {
    if (string.isEmpty) "" + quote + quote
    else if (string.head == quote && string.last == quote) string
    else {
      string.toList.foldLeft(quote.toString) {
        case (s, c) if (c == quote) =>
          s :+ backslash :+ c
        case (s, c) =>
          s :+ c
      } :+ quote
    }
  }

  /**
   * Ensures that the given string is surrounded by double quotes (").
   * Internal quotes will be escaped with a backslash (\). There will be no
   * change if the first and last characters are already quotes.
   */
  def ensureDoubleQuoted(string: String): String = ensureQuoted(string, doubleQuote)

  /**
   * Ensures that the given string is surrounded by single quotes (').
   * Internal quotes will be escaped with a backslash (\). There will be no
   * change if the first and last characters are already quotes.
   */
  def ensureSingleQuoted(string: String): String = ensureQuoted(string, singleQuote)

}

package latis.util

object StringUtils {

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

  /**
   * Removes double quotes from a string that is quoted.
   */
  def removeDoubleQuotes(string: String): String = {
    if (string.head == doubleQuote && string.last == doubleQuote)
      string.substring(1, string.length - 1)
    else string
  }

  /**
   * Ensures that all backslashes in a string are escaped, by converting each single
   * backslash into a double backslash.
   */
  def ensureBackslashEscaped(string: String): String =
    string.replace(backslash.toString, backslash.toString + backslash.toString)

  /**
   * Ensures that all double quotes in a string are escaped, by converting each double quote
   * into a backslash double quote.
   */
  def ensureDoubleQuoteEscaped(string: String): String =
    string.replace(doubleQuote.toString, backslash.toString + doubleQuote.toString)

  /**
   * Ensures that the string is quoted and escaped by always adding new double quotes,
   * and escaping all prior backslashes and quotes
   */
  def ensureQuotedAndEscaped(string: String): String =
    s"${doubleQuote}${ensureDoubleQuoteEscaped(ensureBackslashEscaped(string))}${doubleQuote}"

  /** Implicit class to enable color console output. */
  implicit class ColoredString(s: String) {
    import Console._
    def green: String  = s"${RESET}${GREEN}$s${RESET}"
    def yellow: String = s"${RESET}${YELLOW}$s${RESET}"
    def red: String    = s"${RESET}${RED}$s${RESET}"
  }
}

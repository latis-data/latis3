package latis.input

import latis.model.DataType
import latis.util.LatisException

import scala.util.matching.Regex.Match

/**
 * Uses a formatted string with groups to extract data values from a data record.
 * This must be defined as a 'format' property for this adapter in the config.
 *
 * This adapter also accepts a 'columns' property to rearrange the order of matches.
 */
class FormattedTextAdapter(
  model: DataType,
  config: FormattedTextAdapter.Config
) extends RegexAdapter(model, config) {

  override def extractValues(record: String): Vector[String] =
    super.extractValues(record).map(_.replace(' ', '0'))

}

object FormattedTextAdapter extends AdapterFactory {
  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): FormattedTextAdapter =
    new FormattedTextAdapter(model, FormattedTextAdapter.Config(config.properties *))

  /**
   * Configuration specific to a FormattedTextAdapter.
   */
  class Config private (properties: (String, String)*) extends RegexAdapter.Config(properties *) {
    // This will only be called from the apply method, and so format will always
    // be present.
    val format: String = get("format").get
  }

  object Config {
    def apply(properties: (String, String)*): FormattedTextAdapter.Config = {
      val tempConfig = new TextAdapter.Config(properties *)
      val format: String = tempConfig.propertyMap.getOrElse(
        "format",
        throw LatisException("FormattedTextAdapter requires a format definition.")
      )
      val pattern: String = formatToRegex(format, tempConfig.delimiter)
      new FormattedTextAdapter.Config(properties :+ ("pattern" -> pattern) *)
    }

  }

  private def formatToRegex(format: String, delimiter: String): String = {
    //eg: 3I2 => (?:[ \d]{2}){3}
    //matches "123456"
    val int = (s: String) =>
      """(\d*)I(\d+),?""".r.replaceAllIn(s, (m: Match) => {
        val length = m.group(2)
        val num = m.group(1) match {
          case "" => 1
          case s  => s.toInt
        }
        s"(?:[ \\\\d]{$length}){$num}"
      })
    //eg: 3F7.2 => (?:[ -\d]{4}\.[\d]{2}){3}
    //matches "1234.56-123.45 -12.34"
    val float = (s: String) =>
      """(\d*)F(\d+)\.(\d+),?""".r.replaceAllIn(
        s,
        (m: Match) => {
          val length  = m.group(2).toInt
          val decimal = m.group(3).toInt
          val num = m.group(1) match {
            case "" => 1
            case s  => s.toInt
          }
          s"(?:[- \\\\d]{${length - decimal - 1}}\\\\.[\\\\d]{$decimal}){$num}"
        }
      )
    //eg: 2A3 => (?:.{3}){2}
    //matches "123ABC"
    val string = (s: String) =>
      """(\d*)A(\d*),?""".r.replaceAllIn(s, (m: Match) => {
        val length = m.group(2) match {
          case "" => 1
          case s  => s.toInt
        }
        val num = m.group(1) match {
          case "" => 1
          case s  => s.toInt
        }
        s"(?:.{$length}){$num}"
      })
    //new lines are replaced by delimiters for linesPerRecord > 1,
    //so replace '/' in the format with a delimiter.
    val nl = (s: String) => """/""".r.replaceAllIn(s, delimiter)

    int(float(string(nl(format))))
  }

}

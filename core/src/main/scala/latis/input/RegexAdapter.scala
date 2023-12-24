package latis.input

import scala.util.matching.Regex
import latis.model.DataType
import latis.util.LatisException

/**
 * Uses a regular expression with groups to extract data values from a data record.
 * This must be defined as a 'pattern' property for this adapter in the config.
 *
 * This adapter also accepts a 'columns' property to rearrange the order of matches.
 */
class RegexAdapter(
  model: DataType,
  config: RegexAdapter.Config
) extends TextAdapter(model, config) {

  /**
   * Get the required regular expression pattern from the adapter definition.
   */
  lazy val regex: Regex = config.pattern

  lazy val columnIndices: Option[Vector[Array[Int]]] = config.columns.map { s =>
    s.split(";").map(p => p.split(",").map(_.toInt)).toVector
  }

  /**
   * Return a List of values in the given record that match
   * this Adapter's regular expression pattern.
   * Return an empty List if the record does not match (i.e. does not contain valid data).
   */
  override def extractValues(record: String): Vector[String] = {
    val groups = regex.findFirstMatchIn(record) match {
      case Some(m) => m.subgroups.toVector
      case None    => Vector[String]()
    }

    // If the 'columns' attribute was set, use the indices to reorder
    // the matches. Otherwise, just return the matches in order.
    columnIndices.map { ci =>
      if (groups.length <= ci.flatten.max) {
        // Ignore rows with fewer columns than those requested
        Vector[String]()
      } else {
        // append with " " for now since delimiter could be a regex
        ci.map(is => is.map(groups(_)).mkString(" "))
      }
    }.getOrElse(groups)
  }
}

object RegexAdapter extends AdapterFactory {
  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): RegexAdapter =
    new RegexAdapter(model, new RegexAdapter.Config(config.properties *))

  /**
   * Configuration specific to a RegexAdapter.
   */
  class Config(properties: (String, String)*) extends TextAdapter.Config(properties *) {
    val pattern: Regex = get("pattern")
      .getOrElse(throw LatisException("RegexAdapter requires a pattern definition."))
      .r
    val columns: Option[String] = get("columns")
  }
}

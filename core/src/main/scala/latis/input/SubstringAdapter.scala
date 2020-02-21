package latis.input

import latis.model.DataType
import latis.util.LatisException

/**
 * Uses string index properties to parse ascii tabular data.
 * The specification must be defined as a 'substring' property in the config.
 * Each Variable should have a start and stop index value separated by a comma (,).
 * These will be applied with Java's String.substring (character at final index not included).
 * each set of indices should be separated by a semi-colon (;).
 */
class SubstringAdapter(
  model: DataType,
  config: SubstringAdapter.Config
) extends TextAdapter(model, config) {

  /**
   * Parse the substring indices and store by variable name.
   */
  lazy val substringIndices: Vector[Array[Int]] = config.substring match {
    case s: String => s.split(";").map(p => p.split(",").map(_.toInt)).toVector
  }

  /**
   * Extract the value of each Variable as a String.
   */
  override def extractValues(record: String): Vector[String] = {
    substringIndices.flatMap{ is =>
      val i0 = is(0)
      val i1 = is(1)
      if (i1 > record.length) None  // Ignore rows not long enough
      // TODO: replace with fill value
      else Some(record.substring(i0, i1))
    }
  }
}

object SubstringAdapter extends AdapterFactory {
  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): SubstringAdapter =
    new SubstringAdapter(model, new SubstringAdapter.Config(config.properties: _*))

  /**
   * Configuration specific to a SubstringAdapter.
   */
  class Config(properties: (String, String)*) extends TextAdapter.Config(properties: _*) {
    val substring: String = get("substring")
      .getOrElse(throw LatisException("SubstringAdapter requires a substring definition."))
  }
}

package latis.input

import latis.model.DataType
import latis.util.LatisException

/**
 * Uses column index properties to parse ascii tabular data.
 * Map Variable names to zero-based column index(s).
 * Each variable spec will be separated by ";" and
 * column indices for multi-column variables will
 * be separated by ",". e.g. columns="0,1,2;5;3;4"
 */
class ColumnarAdapter(
  model: DataType,
  config: ColumnarAdapter.Config
) extends TextAdapter(model, config) {
  lazy val columnIndices: Vector[Array[Int]] = config.columns match {
    case s: String =>
      s.split(";").map(p => p.split(",").map(_.toInt)).toVector
  }

  override def extractValues(record: String): Vector[String] = {
    val ss = splitAtDelim(record).filterNot(_.isEmpty)
    // Ignore rows with fewer columns than those requested
    if (ss.length <= columnIndices.flatten.max) Vector()
    // append with " " for now since delimiter could be a regex
    else columnIndices.map(is => is.map(ss(_)).mkString(" "))
  }
}

object ColumnarAdapter extends AdapterFactory {
  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): ColumnarAdapter =
    new ColumnarAdapter(model, new ColumnarAdapter.Config(config.properties: _*))

  /**
   * Configuration specific to a ColumnarAdapter.
   */
  class Config(properties: (String, String)*) extends TextAdapter.Config(properties: _*) {
    val columns: String = get("columns")
      .getOrElse(throw LatisException("ColumnarAdapter requires a columns definition."))
  }
}

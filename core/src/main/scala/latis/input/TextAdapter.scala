package latis.input

import latis.data.Sample
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.util.PropertiesLike
import latis.util.StreamUtils

import java.net.URI

import cats.effect.IO
import fs2.Stream
import fs2.text
import latis.util.ConfigLike

/**
 * Adapter for record-oriented text datasets that can be streamed.
 */
class TextAdapter(model: DataType, config: TextAdapter.Config = TextAdapter.Config())
  extends StreamingAdapter[String] {
  
  /**
   * Provide a Stream of records as Strings.
   * Apply configuration options to the Stream.
   */
  def recordStream(uri: URI): Stream[IO, String] =
    StreamSource.getStream(uri)
      .through(text.utf8Decode)
      .through(text.lines)
      .drop(config.linesToSkip)
      .dropWhile(seekingDataMarker)
      .filter(notComment)
      .filter(_.nonEmpty) //filter out empty lines
      //TODO: avoid the extra work if linesPerRecord = 1 ?
      .chunkN(config.linesPerRecord)
      .map(_.toVector.mkString(config.delimiter)) 
      //TODO: fillDelimiter: can't use delimiter if regex; if contains "\" and "," then "," else " "?
  
      
  /**
   * Given a line of text from the data source, return a Boolean
   * expressing whether we are in the state of seeking the data
   * marker that indicates that the data starts on the next line.
   * This is used by the record Stream to drop lines that do not 
   * need to be parsed.
   */
  private val seekingDataMarker: String => Boolean = (line: String) => {
    var foundDataMarker = false
    config.dataMarker match {
      case Some(marker) => {
        //a little gymnastics to also skip the line with the marker
        val seeking = ! foundDataMarker
        if (line.matches(marker)) foundDataMarker = true
        seeking
      }
      case None => false //no marker defined so we are not seeking
    }
  }

  
  /**
   * Given a line of text from the data source, return a Boolean 
   * expressing if it does NOT start with the optionally configured 
   * comment character.
   * Used by the record Stream to filter out comment lines.
   */
  private val notComment: String => Boolean = (line: String) => {
    config.commentCharacter match {
      case Some(cc) => ! line.startsWith(cc)
      case None     => true //no comment character defined
    }
  }
    
      
  /**
   * Parse a record into a Sample. 
   * Return None if the record is invalid.
   */
  def parseRecord(record: String): Option[Sample] = {
    // We assume one value for each scalar in the model.
    // Note that Samples don't capture nested tuple structure.
    // Assume uncurried model (no nested function), for now.
    /*
     * TODO: Traverse the model to build nested functions (Function in range) 
     * should we apply currying logic or require operation? 
     * curry should be cheap for ordered data
     */
    
    // Get Vectors of domain and range Scalar types.
    val (dtypes, rtypes) = model match {
      case Function(d, r) => (d.getScalars, r.getScalars)
      case _ => (Vector.empty, model.getScalars)
    }
    
    // Extract the data values from the record
    // and split into Vectors of domain and range values.
    val values = extractValues(record)
    val (dvals, rvals) = values.splitAt(dtypes.length)
    
    // Zip the types with the values, then construct a Sample 
    // from the parsed domain and range values.
    if (rtypes.length != rvals.length) None //invalid record
    else {
      val ds = (dtypes zip dvals).map(p => p._1.parseValue(p._2))
      val rs = (rtypes zip rvals).map(p => p._1.parseValue(p._2))
      Some(Sample(ds, rs))
    }
  }
  
  /**
   * Extract the data values from the given record.
   */
  def extractValues(record: String): Vector[String] = 
    splitAtDelim(record)
  
  /**
   * Split the given string based on the configured delimiter.
   * The delimiter can be a regular expression.
   * A trailing delimiter will yield an empty string.
   */
  def splitAtDelim(str: String): Vector[String] = 
    str.trim.split(config.delimiter, -1).toVector
    //Note, use "-1" so trailing ","s will yield empty strings.
    //TODO: StringUtil?
}

//=============================================================================

object TextAdapter extends AdapterFactory {
  
  def apply(model: DataType, config: Config = Config()): TextAdapter = 
    new TextAdapter(model, config)
  
  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): TextAdapter = 
    new TextAdapter(model, Config(config.properties: _*))
  
  /**
   * Configuration specific to a TextAdapter.
   */
  case class Config(properties: (String, String)*) extends ConfigLike {
    val commentCharacter: Option[String] = get("commentCharacter")
    val delimiter: String                = getOrElse("delimiter", ",")
    val linesPerRecord: Int              = getOrElse("linesPerRecord", 1)
    val linesToSkip: Int                 = getOrElse("skipLines", 0)
    val dataMarker: Option[String]       = get("dataMarker")
  }
  
}

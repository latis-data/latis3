package latis.data

/**
 * Wrapper for dependent variables in a Sample.
 * Note that this uses varargs so construction and extraction
 * allow comma separated data values.
 */
case class RangeData(data: Any*)

object RangeData {
  def fromSeq(data: Seq[_]): RangeData = RangeData(data: _*)
}
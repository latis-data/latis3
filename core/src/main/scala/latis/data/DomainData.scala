package latis.data

/**
 * Wrapper for independent variables in a Sample.
 * Note that this uses varargs so construction and extraction
 * allow comma separated data values.
 */
case class DomainData(data: Any*)

object DomainData {
  def fromSeq(data: Seq[_]): DomainData = DomainData(data: _*)
}


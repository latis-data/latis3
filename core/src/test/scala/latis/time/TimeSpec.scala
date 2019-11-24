package latis.time

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.Data
import latis.metadata.Metadata

class TimeSpec extends FlatSpec {
  
  val formattedTime: Time = Time(
    Metadata(
      "id"    -> "time",
      "type"  -> "string",
      "units" -> "MMM dd, yyyy"
    )
  )
  
  val numericTime: Time = Time(
    Metadata(
      "id"    -> "time",
      "type"  -> "double",
      "units" -> "seconds since 2000-01-01"
    )
  )
      
  
  "A numeric time" should "parse a string as a number" in {
    numericTime.parseValue("86400") match {
      case Right(d: Data.DoubleValue) =>
        d.value should be (86400d)
    }
  }

  "A formatted time" should "convert an ISO time" in {
    formattedTime.convertValue("2000-001") match {
      case Right(d: Data.StringValue) =>
        d.value should be("Jan 01, 2000")
    }
  }
  
  "Time ordering" should "compare two formatted time values" in {
    formattedTime.ordering.compare("Jan 01, 2000", "Feb 01, 2000") should be (-1)
  }

}

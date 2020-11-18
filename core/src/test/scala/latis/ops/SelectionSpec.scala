package latis.ops

import latis.data._
import latis.metadata.Metadata
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import latis.model._
import latis.time.Time


class SelectionSpec extends FlatSpec {

  "A time selection" should "filter formatted times" in {
    val model = Function(
      Time(Metadata("id" -> "time", "units" -> "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "type" -> "string")),
      Scalar(Metadata("id" -> "a", "type" -> "int"))
    )

    val sample = Sample(
      DomainData("2000-01-01T00:00:00.000Z"),
      RangeData(1)
    )

    Selection.makeSelection("time > 1999").fold(throw _, identity).predicate(model)(sample) should be (true)
    Selection.makeSelection("time > 2001").fold(throw _, identity).predicate(model)(sample) should be (false)
  }
}

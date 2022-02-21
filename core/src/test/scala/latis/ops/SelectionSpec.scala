package latis.ops

import latis.data._
import latis.metadata.Metadata
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.model._
import latis.time.Time
import latis.util.Identifier.IdentifierStringContext


class SelectionSpec extends AnyFlatSpec {

  "A time selection" should "filter formatted times" in {
    val time = Time.fromMetadata(
      Metadata(
        "id" -> "time",
        "type" -> "string",
        "units" -> "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
      )
    ).value

    val model = Function.from(
      time,
      Scalar(id"a", IntValueType)
    ).value

    val sample = Sample(
      DomainData("2000-01-01T00:00:00.000Z"),
      RangeData(1)
    )

    Selection.makeSelection("time > 1999").value.predicate(model).value(sample) should be (true)
    Selection.makeSelection("time > 2001").value.predicate(model).value(sample) should be (false)
  }
}

package latis.ops

import cats.effect.*
import fs2.Stream
import munit.CatsEffectSuite

import latis.data.*
import latis.metadata.Metadata
import latis.model.*
import latis.util.Identifier.id

class CaresianatorSuite extends CatsEffectSuite {

  private val model = (for {
    domain <- Tuple.fromElements(
      Scalar(id"x", IntValueType),
      Scalar(id"y", IntValueType)
    )
    a <- Scalar.fromMetadata(Metadata(
      "id" -> "a",
      "type" -> "int",
      "fillValue" -> "-999"
    ))
    f <- Function.from(domain, a)
  } yield f).fold(throw _, identity)

  test("test") {
    val samples = List(
      Sample(DomainData(0,1), RangeData(1)),
      Sample(DomainData(1,0), RangeData(2))
    )
    val expected = List(
      Sample(DomainData(0,0), RangeData(-999)),
      Sample(DomainData(0,1), RangeData(1)),
      Sample(DomainData(1,0), RangeData(2)),
      Sample(DomainData(1,1), RangeData(-999))
    )
    Cartesianator().pipe(model)(Stream.emits(samples)).compile.toList.map { ss =>
      assertEquals(ss, expected)
    }
  }

}

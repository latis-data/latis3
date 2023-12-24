package latis.util

import scala.collection.mutable

import munit.FunSuite

import latis.data.*
import latis.metadata.Metadata
import latis.model.Function
import latis.model.IntValueType
import latis.model.Scalar
import latis.model.Tuple
import latis.time.Time
import latis.util.Identifier.*

class OrderingSuite extends FunSuite {

  private val time = Time.fromMetadata(
    Metadata(
      "id" -> "time",
      "type" -> "string",
      "units" -> "MM/dd/yyyy"
    )
  )

  private val model = time.flatMap {
    Tuple.fromElements(Scalar(id"x", IntValueType), _)
  }.flatMap {
    Function.from(_, Scalar(id"a", IntValueType))
  }.fold(fail("Failed to construct model", _), identity)

  private val samples = List(
    Sample(DomainData(0, "01/01/2001"), RangeData(2)),
    Sample(DomainData(1, "01/01/2001"), RangeData(4)),
    Sample(DomainData(1, "02/01/2000"), RangeData(3)),
    Sample(DomainData(0, "02/01/2000"), RangeData(1)),
  )

  test("2D samples with time should be sortable") {

    val totalOrdering = LatisOrdering.partialToTotal(LatisOrdering.sampleOrdering(model))

    val res = samples.sorted(totalOrdering).map {
      case Sample(_, RangeData(Integer(x))) => x
      case _ => fail("unexpected sample")
    }

    assertEquals(res, List(1L, 2L, 3L, 4L))
  }

  test("2D samples with time should go into a SortedMap ordered by keys") {
    val ordering = LatisOrdering.partialToTotal(CartesianDomainOrdering(model.domain.getScalars.map(_.ordering)))

    val smap = mutable.SortedMap[DomainData, RangeData]()(ordering)
    samples.foreach {
      case Sample(dd, rd) => smap += (dd -> rd)
    }

    val res = smap.map {
      case (_, RangeData(Integer(x))) => x
      case _ => fail("unexpected sample")
    }

    assertEquals(res.toList, List(1L, 2L, 3L, 4L))
  }
}

package latis.model

import cats.syntax.all.*
import munit.FunSuite

import latis.data.*
import latis.metadata.Metadata
import latis.util.Identifier.*

class FillDataSuite extends FunSuite {

  private lazy val nonNullableScalar = Scalar(id"nns", IntValueType)
  private lazy val scalarWithFill    = Scalar.fromMetadata(Metadata("id" -> "swf", "type" -> "int", "fillValue" -> "-1"))
    .fold(fail("failed to construct scalar", _), identity)
  private lazy val scalarWithMissing = Scalar.fromMetadata(Metadata("id" -> "swm", "type" -> "int", "missingValue" -> "-9"))
    .fold(fail("failed to construct scalar", _), identity)
  private lazy val scalarWithFillAndMissing =
    Scalar.fromMetadata(Metadata("id" -> "swfm", "type" -> "int", "fillValue" -> "-1", "missingValue" -> "-9"))
      .fold(fail("failed to construct scalar", _), identity)

  private lazy val tuple: Tuple =
    (
      nonNullableScalar.asRight,
      scalarWithFill.asRight,
      Tuple.fromElements(
        scalarWithMissing,
        scalarWithFillAndMissing
      ),
      Function.from(nonNullableScalar, scalarWithFill)
    ).flatMapN(Tuple.fromElements(_, _, _, _))
      .fold(fail("failed to construct tuple", _), identity)

  test("fillable only if fillValue is defined") {
    assert(!nonNullableScalar.isFillable)
    assert(scalarWithFill.isFillable)
    assert(!scalarWithMissing.isFillable)
    assert(scalarWithFillAndMissing.isFillable)
  }

  test("fill non-nullable Scalar with null") {
    assertEquals(nonNullableScalar.fillData, NullData)
  }

  test("fill Scalar with fill value") {
    assertEquals(scalarWithFill.fillData, Data.IntValue(-1))
  }

  test("fill Scalar with missing value") {
    assertEquals(scalarWithMissing.fillData, Data.IntValue(-9))
  }

  test("fill Scalar with fill value over missing") {
    assertEquals(scalarWithFillAndMissing.fillData, Data.IntValue(-1))
  }

  test("tuple") {
    tuple.fillData match {
      case TupleData(NullData, Integer(f), Integer(m), Integer(fm), NullData) =>
        assertEquals(f, -1L)
        assertEquals(m, -9L)
        assertEquals(fm, -1L)
      case _ => fail("unexpected TupleData")
    }
  }
}

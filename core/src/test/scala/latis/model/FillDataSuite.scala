package latis.model

import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.data._
import latis.metadata.Metadata
import latis.util.Identifier.IdentifierStringContext

class FillDataSuite extends AnyFunSuite {

  private val nonNullableScalar = Scalar(id"nns", IntValueType)
  private val scalarWithFill    = Scalar.fromMetadata(Metadata("id" -> "swf", "type" -> "int", "fillValue" -> "-1")).value
  private val scalarWithMissing = Scalar.fromMetadata(Metadata("id" -> "swm", "type" -> "int", "missingValue" -> "-9")).value
  private val scalarWithFillAndMissing =
    Scalar.fromMetadata(Metadata("id" -> "swfm", "type" -> "int", "fillValue" -> "-1", "missingValue" -> "-9")).value

  private val tuple: Tuple = Tuple.fromElements(
    nonNullableScalar,
    scalarWithFill,
    Tuple.fromElements(
      scalarWithMissing,
      scalarWithFillAndMissing
    ).value,
    Function.from(nonNullableScalar, scalarWithFill).value
  ).value

  test("fillable only if fillValue is defined") {
    assert(!nonNullableScalar.isFillable)
    assert(scalarWithFill.isFillable)
    assert(!scalarWithMissing.isFillable)
    assert(scalarWithFillAndMissing.isFillable)
  }

  test("fill non-nullable Scalar with null") {
    inside(nonNullableScalar.fillData) {
      case NullData => assert(true)
    }
  }

  test("fill Scalar with fill value") {
    inside(scalarWithFill.fillData) {
      case Integer(fv) => assert(fv == -1)
    }
  }

  test("fill Scalar with missing value") {
    inside(scalarWithMissing.fillData) {
      case Integer(fv) => assert(fv == -9)
    }
  }

  test("fill Scalar with fill value over missing") {
    inside(scalarWithFillAndMissing.fillData) {
      case Integer(fv) => assert(fv == -1)
    }
  }

  test("tuple") {
    inside(tuple.fillData) {
      case TupleData(NullData, Integer(f), Integer(m), Integer(fm), NullData) =>
        assert(f  == -1)
        assert(m  == -9)
        assert(fm == -1)
    }
  }
}

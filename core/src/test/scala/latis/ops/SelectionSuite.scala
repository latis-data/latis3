package latis.ops

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.EitherValues._

import latis.data.Data
import latis.metadata.Metadata
import latis.model.Scalar
import latis.time.Time
import latis.util.dap2.parser.ast

class SelectionSuite extends AnyFunSuite {
  //TODO: test regular selections...

  //-- Selection with bin semantics --//

  lazy val binnedScalar = Scalar.fromMetadata(Metadata(
    "id" -> "x",
    "type" -> "double",
    "binWidth" -> "1"
  )).value

  // Data value at start of bin: [1, 2)
  lazy val datum = Data.DoubleValue(1.0)

  test("bin eq at start") {
    val value = Data.DoubleValue(1.0)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.Eq, value)(datum))
  }
  test("bin eq in bin") {
    val value = Data.DoubleValue(1.5)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.Eq, value)(datum))
  }
  test("bin not eq at end") {
    val value = Data.DoubleValue(2.0)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.Eq, value)(datum))
  }
  test("bin not eq before") {
    val value = Data.DoubleValue(0.5)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.Eq, value)(datum))
  }
  test("bin not eq after") {
    val value = Data.DoubleValue(2.5)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.Eq, value)(datum))
  }

  test("bin gt before") {
    val value = Data.DoubleValue(0.5)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.Gt, value)(datum))
  }
  test("bin not gt start") {
    val value = Data.DoubleValue(1.0)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.Gt, value)(datum))
  }
  test("bin not gt in bin") {
    val value = Data.DoubleValue(1.5)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.Gt, value)(datum))
  }
  test("bin not gt end") {
    val value = Data.DoubleValue(2.0)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.Gt, value)(datum))
  }
  test("bin not gt after") {
    val value = Data.DoubleValue(2.5)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.Gt, value)(datum))
  }

  test("bin gteq before") {
    val value = Data.DoubleValue(0.5)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.GtEq, value)(datum))
  }
  test("bin gteq start") {
    val value = Data.DoubleValue(1.0)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.GtEq, value)(datum))
  }
  test("bin gteq in bin") {
    val value = Data.DoubleValue(1.5)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.GtEq, value)(datum))
  }
  test("bin not gteq end") {
    val value = Data.DoubleValue(2.0)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.GtEq, value)(datum))
  }
  test("bin not gteq after") {
    val value = Data.DoubleValue(2.5)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.GtEq, value)(datum))
  }

  test("bin not lt before") {
    val value = Data.DoubleValue(0.5)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.Lt, value)(datum))
  }
  test("bin not lt start") {
    val value = Data.DoubleValue(1.0)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.Lt, value)(datum))
  }
  test("bin not lt in bin") {
    val value = Data.DoubleValue(1.5)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.Lt, value)(datum))
  }
  test("bin lt end") {
    val value = Data.DoubleValue(2.0)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.Lt, value)(datum))
  }
  test("bin lt after") {
    val value = Data.DoubleValue(2.5)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.Lt, value)(datum))
  }

  test("bin not lteq before") {
    val value = Data.DoubleValue(0.5)
    assert(! Selection.datumPredicateWithBinning(binnedScalar, ast.LtEq, value)(datum))
  }
  test("bin lteq start") {
    val value = Data.DoubleValue(1.0)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.LtEq, value)(datum))
  }
  test("bin lteq in bin") {
    val value = Data.DoubleValue(1.5)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.LtEq, value)(datum))
  }
  test("bin lteq end") {
    val value = Data.DoubleValue(2.0)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.LtEq, value)(datum))
  }
  test("bin lteq after") {
    val value = Data.DoubleValue(2.5)
    assert(Selection.datumPredicateWithBinning(binnedScalar, ast.LtEq, value)(datum))
  }

  //-- Time selection with bin semantics --//

  private lazy val binnedNumericTime = Time.fromMetadata(Metadata(
    "id"       -> "time",
    "type"     -> "double",
    "units"    -> "seconds since 2000-01-01",
    "binWidth" -> "1" //seconds, matching units
  )).value

  private lazy val binnedNonIsoFormattedTime: Time = Time.fromMetadata(Metadata(
    "id"       -> "time",
    "type"     -> "string",
    "units"    -> "MMM dd, yyyy",
    "binWidth" -> "100000000" //ms, native time units
  )).value

  test("binnedNumericTime eq in bin") {
    val datum = Data.DoubleValue(2.0)
    val value = Data.DoubleValue(2.5)
    assert(Selection.datumPredicateWithBinning(binnedNumericTime, ast.Eq, value)(datum))
  }

  test("binnedNonIsoFormattedTime eq in bin") {
    val datum = Data.StringValue("Jan 01, 1970")
    val value = Data.StringValue("Jan 02, 1970")
    assert(Selection.datumPredicateWithBinning(binnedNonIsoFormattedTime, ast.Eq, value)(datum))
  }
}

package latis.model

import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.data.Data
import latis.data.NullData
import latis.metadata.Metadata
import latis.model.IntValueType
import latis.util.Identifier.IdentifierStringContext

class ScalarFactorySuite extends AnyFunSuite {

  //---- id ----//

  test("id") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int")) match {
      case Right(s) => assert(s.id == id"a")
      case Left(le) => fail(s"Construction with id failed: ${le.message}")
    }
  }

  test("no id") {
    inside(Scalar.fromMetadata(Metadata())) {
      case Left(le) => assert(le.message == "No id defined")
    }
  }

  test("invalid id") {
    inside(Scalar.fromMetadata(Metadata("id" -> "123"))) {
      case Left(le) => assert(le.message == "Invalid id: 123")
    }
  }

  //---- type ----//

  test("value type") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int")) match {
      case Right(s) => assert(s.valueType == IntValueType)
      case Left(le) => fail(s"Construction with type failed: ${le.message}")
    }
  }

  test("no value type") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a"))) {
      case Left(le) => assert(le.message == "No type defined")
    }
  }

  test("invalid value type") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "foo"))) {
      case Left(le) => assert(le.message == "Invalid Scalar value type: foo")
    }
  }

  //---- missing ----//

  test("missing") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "missingValue" -> "-999"))) {
      case Right(s) => assert(s.missingValue.contains(Data.IntValue(-999)))
    }
  }

  test("null as missing") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "missingValue" -> "null"))) {
      case Right(s) => assert(s.missingValue.contains(NullData))
    }
  }

  test("invalid missing") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "missingValue" -> "-9.9"))) {
      case Left(le) => assert(le.message == "Failed to parse IntValue: -9.9")
    }
  }

  //---- fill ----//

  test("fill") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "fillValue" -> "-999"))) {
      case Right(s) => assert(s.fillValue.contains(Data.IntValue(-999)))
    }
  }

  test("no null as fill") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "fillValue" -> "null"))) {
      case Left(le) => assert(le.message == "FillValue must not be 'null'")
    }
  }

  test("invalid fill") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "fillValue" -> "-9.9"))) {
      case Left(le) => assert(le.message == "Failed to parse IntValue: -9.9")
    }
  }

  //---- precision ----//

  test("precision must be non-negative") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "double", "precision" -> "-1"))) {
      case Left(le) => assert(le.message == "Precision must not be negative")
    }
  }

  test("precision must be an integer") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "double", "precision" -> "1.2"))) {
      case Left(le) => assert(le.message == "Precision must be an integer")
    }
  }

  test("invalid value type for precision") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "precision" -> "2"))) {
      case Left(le) => assert(le.message == "Precision is not supported for value type: int")
    }
  }

  //---- order ----//

  test("default ascending") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int"))) {
      case Right(s) => assert(s.ascending)
    }
  }

  test("explicit ascending") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "order" -> "asc"))) {
      case Right(s) => assert(s.ascending)
    }
  }

  test("descending") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "order" -> "desc"))) {
      case Right(s) => assert(!s.ascending)
    }
  }

  test("invalid order") {
    inside(Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "order" -> "foo"))) {
      case Left(le) => assert(le.message == "Order must be 'asc' or 'desc'")
    }
  }

  //---- time ----//

  test("construct time") {
    val md = Metadata("id" -> "a", "type" -> "string", "units" -> "yyyy", "class" -> "latis.time.Time")
    Scalar.fromMetadata(md).value.isInstanceOf[latis.time.Time]
  }
}

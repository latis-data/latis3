package latis.model

import munit.FunSuite

import latis.data.Data
import latis.data.NullData
import latis.metadata.Metadata
import latis.model.IntValueType
import latis.util.Identifier._

class ScalarFactorySuite extends FunSuite {

  //---- id ----//

  test("id") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int")) match {
      case Right(s) => assertEquals(s.id, id"a")
      case Left(le) => fail(s"Construction with id failed: ${le.message}")
    }
  }

  test("no id") {
    Scalar.fromMetadata(Metadata()) match {
      case Left(le) => assertEquals(le.message, "No id defined")
      case _ => fail("Constructed Scalar with no ID")
    }
  }

  test("invalid id") {
    Scalar.fromMetadata(Metadata("id" -> "123")) match {
      case Left(le) => assertEquals(le.message, "Invalid id: 123")
      case _ => fail("Constructed Scalar with invalid ID")
    }
  }

  //---- type ----//

  test("value type") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int")) match {
      case Right(s) => assertEquals(s.valueType, IntValueType)
      case Left(le) => fail(s"Construction with type failed: ${le.message}")
    }
  }

  test("no value type") {
    Scalar.fromMetadata(Metadata("id" -> "a")) match {
      case Left(le) => assertEquals(le.message, "No type defined")
      case _ => fail("Constructed Scalar without type")
    }
  }

  test("invalid value type") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "foo")) match {
      case Left(le) => assertEquals(le.message, "Invalid Scalar value type: foo")
      case _ => fail("Constructed Scalar with invalid type")
    }
  }

  //---- missing ----//

  test("missing") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "missingValue" -> "-999")) match {
      case Right(s) => assert(s.missingValue.contains(Data.IntValue(-999)))
      case _ => fail("Failed to construct Scalar")
    }
  }

  test("null as missing") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "missingValue" -> "null")) match {
      case Right(s) => assert(s.missingValue.contains(NullData))
      case _ => fail("Failed to construct Scalar")
    }
  }

  test("invalid missing") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "missingValue" -> "-9.9")) match {
      case Left(le) => assertEquals(le.message, "Failed to parse IntValue: -9.9")
      case _ => fail("Constructed Scalar with invalid missing value")
    }
  }

  //---- fill ----//

  test("fill") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "fillValue" -> "-999")) match {
      case Right(s) => assert(s.fillValue.contains(Data.IntValue(-999)))
      case _ => fail("Failed to construct Scalar")
    }
  }

  test("no null as fill") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "fillValue" -> "null")) match {
      case Left(le) => assertEquals(le.message, "FillValue must not be 'null'")
      case _ => fail("Constructed Scalar with null fill value")
    }
  }

  test("invalid fill") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "fillValue" -> "-9.9")) match {
      case Left(le) => assertEquals(le.message, "Failed to parse IntValue: -9.9")
      case _ => fail("Constructed Scalar with invalid fill value")
    }
  }

  //---- precision ----//

  test("precision must be non-negative") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "double", "precision" -> "-1")) match {
      case Left(le) => assertEquals(le.message, "Precision must not be negative")
      case _ => fail("Constructed Scalar with negative precision")
    }
  }

  test("precision must be an integer") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "double", "precision" -> "1.2")) match {
      case Left(le) => assertEquals(le.message, "Precision must be an integer")
      case _ => fail("Constructed Scalar with non-integer precision")
    }
  }

  test("invalid value type for precision") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "precision" -> "2")) match {
      case Left(le) => assertEquals(le.message, "Precision is not supported for value type: int")
      case _ => fail("Constructed Scalar with precision for unsupported type")
    }
  }

  //---- order ----//

  test("default ascending") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int")) match {
      case Right(s) => assert(s.ascending)
      case _ => fail("Failed to construct Scalar")
    }
  }

  test("explicit ascending") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "order" -> "asc")) match {
      case Right(s) => assert(s.ascending)
      case _ => fail("Failed to construct Scalar")
    }
  }

  test("descending") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "order" -> "desc")) match {
      case Right(s) => assert(!s.ascending)
      case _ => fail("Failed to construct Scalar")
    }
  }

  test("invalid order") {
    Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "int", "order" -> "foo")) match {
      case Left(le) => assertEquals(le.message, "Order must be 'asc' or 'desc'")
      case _ => fail("Constructed Scalar with invalid order")
    }
  }

  //---- time ----//

  test("construct time") {
    val md = Metadata("id" -> "a", "type" -> "string", "units" -> "yyyy", "class" -> "latis.time.Time")
    val scalar = Scalar.fromMetadata(md).fold(fail("Failed to construct Scalar", _), identity)

    assert(scalar.isInstanceOf[latis.time.Time])
  }
}

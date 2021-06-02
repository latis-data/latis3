package latis.input

import org.scalatest.funsuite.AnyFunSuite

import latis.dsl._
import latis.metadata.Metadata
import latis.model._
import latis.ops._
import latis.time._
import latis.util.Identifier._

class JdbcAdapterSuite extends AnyFunSuite {

  val config = JdbcAdapter.Config(
    "driver" -> "myDriver",
    "table" -> "myTable",
    "user" -> "myUser",
    "password" -> "myPassword"
  )

  val model = ModelParser.unsafeParse("(x, y) -> (a, b, c)")

  val adapter = JdbcAdapter(model, config)

  //---- SQL from Operations ----//

  test("make sql with no operations") {
    val ops = List()
    val sql = adapter.buildQuery(ops)
    assert(sql == "SELECT x, y, a, b, c FROM myTable ORDER BY x, y ASC")
  }

  test("make sql with selections and projection without domain") {
    val ops = List(
      Selection.makeSelection("x > 1").toTry.get,
      Selection.makeSelection("a >= 2").toTry.get,
      Projection.fromExpression("b, c").toTry.get
    )
    val sql = adapter.buildQuery(ops)
    assert(sql == "SELECT b, c FROM myTable WHERE x > 1 AND a >= 2 ORDER BY x, y ASC")
  }

  test("preserve model variable order") {
    val ops = List(
      Projection.fromExpression("b, a").toTry.get
    )
    val sql = adapter.buildQuery(ops)
    assert(sql.contains("a, b"))
  }

  test("rename some with duplicate") {
    val ops = List (
      Rename(id"b", id"B"),
      Rename(id"x", id"X"),
      Rename(id"b", id"Z"),
    )
    val sql = adapter.buildQuery(ops)
    assert(sql.contains("x AS X"))
    assert(sql.contains("b AS Z")) //last rename wins
  }

  //---- SQL with Time selections ----//

  val modelWithNumericTime = Function(
    Time(Metadata("id" -> "t", "type" -> "int", "units" -> "days since 1970-01-01")),
    Scalar(Metadata("id" -> "a", "type" -> "int"))
  )

  val modelWithTextTime = Function(
    Time(Metadata("id" -> "t", "type" -> "string", "units" -> "yyyy-MM-dd")),
    Scalar(Metadata("id" -> "a", "type" -> "int"))
  )

  test("numeric time selection with numeric time") {
    val ops = List(
      Selection.makeSelection("t > 1").toTry.get
    )
    val sql = JdbcAdapter(modelWithNumericTime, config).buildQuery(ops)
    assert(sql.contains("t > 1"))
  }

  test("ISO time selection with numeric time") {
    val ops = List(
      Selection.makeSelection("t > 1970-01-02").toTry.get
    )
    val sql = JdbcAdapter(modelWithNumericTime, config).buildQuery(ops)
    assert(sql.contains("t > 1"))
  }

  test("ISO time selection with formatted time") {
    val ops = List(
      Selection.makeSelection("t > 1970002").toTry.get
    )
    val sql = JdbcAdapter(modelWithTextTime, config).buildQuery(ops)
    assert(sql.contains("t > '1970-01-02'"))
  }

  //---- Test getLimit ----//

  test("no limit") {
    val ops = List()
    assert(adapter.getLimit(ops).isEmpty)
  }

  test("limit with take") {
    val ops = List(Take(10))
    assert(adapter.getLimit(ops).contains(10))
  }

  test("limit with head") {
    val ops = List(Head())
    assert(adapter.getLimit(ops).contains(1))
  }

  test("limit with take and head") {
    val ops = List(Take(10), Head())
    assert(adapter.getLimit(ops).contains(1))
  }

  test("limit with head and take") {
    val ops = List(Head(), Take(10))
    assert(adapter.getLimit(ops).contains(1))
  }

  test("limit with 0 then head") {
    val ops = List(Take(0), Head())
    assert(adapter.getLimit(ops).contains(0))
  }

}

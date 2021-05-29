package latis.input

import org.scalatest.funsuite.AnyFunSuite

import latis.model.ModelParser
import latis.ops._
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
    assert(sql.contains("b AS Z"))
  }

  //---- Test getLimit ----//

  test("no limit") {
    val ops = List()
    assert(adapter.getLimit(ops) == None)
  }

  test("limit with take") {
    val ops = List(Take(10))
    assert(adapter.getLimit(ops) == Some(10))
  }

  test("limit with head") {
    val ops = List(Head())
    assert(adapter.getLimit(ops) == Some(1))
  }

  test("limit with take and head") {
    val ops = List(Take(10), Head())
    assert(adapter.getLimit(ops) == Some(1))
  }

  test("limit with head and take") {
    val ops = List(Head(), Take(10))
    assert(adapter.getLimit(ops) == Some(1))
  }

  test("limit with 0 then head") {
    val ops = List(Take(0), Head())
    assert(adapter.getLimit(ops) == Some(0))
  }

  //----  ----//
}

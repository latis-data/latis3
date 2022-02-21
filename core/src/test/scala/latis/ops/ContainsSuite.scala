package latis.ops

import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.EitherValues._

import latis.dsl._

class ContainsSuite extends AnyFunSuite {

  /**
   * Defines a Dataset with three samples: (0,a), (1,b), (2,c).
   */
  private lazy val ds = DatasetGenerator("x -> a: string")

  test("contains single value") {
    val c = Contains.fromArgs(List("x", "1")).value
    ds.withOperation(c).samples.compile.toList.map { list =>
      assertResult(1)(list.length)
    }.unsafeRunSync()
  }

  test("contains two values") {
    val c = Contains.fromArgs(List("x", "1", "2")).value
    ds.withOperation(c).samples.compile.toList.map { list =>
      assertResult(2)(list.length)
    }.unsafeRunSync()
  }

  test("contains no values") {
    val c = Contains.fromArgs(List("x", "9", "10")).value
    ds.withOperation(c).samples.compile.toList.map { list =>
      assertResult(0)(list.length)
    }.unsafeRunSync()
  }

  test("contains text value") {
    val c = Contains.fromArgs(List("a", "b")).value
    ds.withOperation(c).samples.compile.toList.map { list =>
      assertResult(1)(list.length)
    }.unsafeRunSync()
  }

  test("contains quoted text value") {
    val c = Contains.fromArgs(List("a", """"b"""")).value
    ds.withOperation(c).samples.compile.toList.map { list =>
      assertResult(1)(list.length)
    }.unsafeRunSync()
  }

  test("application fails with invalid value types") {
    val c = Contains.fromArgs(List("x", "foo")).value
    ds.withOperation(c).samples.attempt.compile.toList.map { list =>
      assert(list.head.isLeft)
    }.unsafeRunSync()
  }

  test("application fails with missing id") {
    val c = Contains.fromArgs(List("z", "foo")).value
    ds.withOperation(c).samples.attempt.compile.toList.map { list =>
      assert(list.head.isLeft)
    }.unsafeRunSync()
  }

  test("construction fails with invalid id") {
    val c = Contains.fromArgs(List("!x", "foo"))
    assert(c.isLeft)
  }
}

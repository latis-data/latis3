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

  test("contains text value") {
    val c = Contains.fromArgs(List("a", "b")).value
    ds.withOperation(c).samples.compile.toList.map { list =>
      assertResult(1)(list.length)
    }.unsafeRunSync()
  }

  ignore("contains quoted text value") {
    val c = Contains.fromArgs(List("a", """"b"""")).value
    ds.withOperation(c).samples.compile.toList.map { list =>
      assertResult(1)(list.length)
    }.unsafeRunSync()
  }
}

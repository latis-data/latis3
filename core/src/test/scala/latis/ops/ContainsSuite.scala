package latis.ops

import munit.CatsEffectSuite

import latis.dsl.*

class ContainsSuite extends CatsEffectSuite {

  /**
   * Defines a Dataset with three samples: (0,a), (1,b), (2,c).
   */
  private lazy val ds = DatasetGenerator("x -> a: string")

  test("contains single value") {
    val c = Contains.fromArgs(List("x", "1"))
      .fold(fail("failed to construct operation", _), identity)

    ds.withOperation(c).samples.compile.toList.map { list =>
      assertEquals(list.length, 1)
    }
  }

  test("contains two values") {
    val c = Contains.fromArgs(List("x", "1", "2"))
      .fold(fail("failed to construct operation", _), identity)

    ds.withOperation(c).samples.compile.toList.map { list =>
      assertEquals(list.length, 2)
    }
  }

  test("contains no values") {
    val c = Contains.fromArgs(List("x", "9", "10"))
      .fold(fail("failed to construct operation", _), identity)

    ds.withOperation(c).samples.compile.toList.map { list =>
      assertEquals(list.length, 0)
    }
  }

  test("contains text value") {
    val c = Contains.fromArgs(List("a", "b"))
      .fold(fail("failed to construct operation", _), identity)

    ds.withOperation(c).samples.compile.toList.map { list =>
      assertEquals(list.length, 1)
    }
  }

  test("contains quoted text value") {
    val c = Contains.fromArgs(List("a", """"b""""))
      .fold(fail("failed to construct operation", _), identity)

    ds.withOperation(c).samples.compile.toList.map { list =>
      assertEquals(list.length, 1)
    }
  }

  test("application fails with invalid value types") {
    val c = Contains.fromArgs(List("x", "foo"))
      .fold(fail("failed to construct operation", _), identity)

    ds.withOperation(c).samples.attempt.compile.toList.map { list =>
      assert(list.head.isLeft)
    }
  }

  test("application fails with missing id") {
    val c = Contains.fromArgs(List("z", "foo"))
      .fold(fail("failed to construct operation", _), identity)

    ds.withOperation(c).samples.attempt.compile.toList.map { list =>
      assert(list.head.isLeft)
    }
  }

  test("construction fails with invalid id") {
    val c = Contains.fromArgs(List("!x", "foo"))
    assert(c.isLeft)
  }
}

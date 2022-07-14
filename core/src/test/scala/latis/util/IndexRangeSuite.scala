package latis.util

import munit.FunSuite

class IndexRangeSuite extends FunSuite {

  //--- FiniteRange ---//

  test("construct finite range") {
    val r = IndexRange.finite(1, 3, 2).fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "1:3:2")
  }

  test("construct finite range with default step") {
    val r = IndexRange.finite(1, 3).fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "1:3:1")
  }

  test("construct finite range with truncation") {
    val r = IndexRange.finite(1, 4, 2).fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "1:3:2")
  }

  test("construct finite range with start = end") {
    val r = IndexRange.finite(1, 1, 2).fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "1:1:2")
  }

  test("fail to construct finite range with start after end") {
    assert(IndexRange.finite(1, 0, 2).isLeft)
  }

  test("fail to construct finite range with negative start") {
    assert(IndexRange.finite(-1, 3, 2).isLeft)
  }

  test("fail to construct finite range with negative step") {
    assert(IndexRange.finite(1, 3, -2).isLeft)
  }

  test("fail to construct finite range with step = 0") {
    assert(IndexRange.finite(1, 3, 0).isLeft)
  }

  test("finite range length") {
    val r = IndexRange.finite(1, 3, 2).fold(e => fail(e.getMessage), identity)
    assertEquals(r.length, Some(2))
  }

  test("finite range is not unlimited") {
    val r = IndexRange.finite(1, 1, 2).fold(e => fail(e.getMessage), identity)
    assert(!r.isUnlimited)
  }

  test("finite range is not emmpty") {
    val r = IndexRange.finite(1, 1, 2).fold(e => fail(e.getMessage), identity)
    assert(!r.isEmpty)
  }

  test("finite stride") {
    val r = IndexRange.finite(1, 5, 2)
      .flatMap(_.stride(2))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "1:5:4")
  }

  test("non-positive finite stride fails") {
    assert(IndexRange.finite(1, 5, 2).flatMap(_.stride(0)).isLeft)
  }

  test("finite stride with truncation") {
    val r = IndexRange.finite(1, 7, 2)
      .flatMap(_.stride(2))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "1:5:4")
  }

  test("stride too large") {
    assert(IndexRange.finite(1, Int.MaxValue, Int.MaxValue / 10)
      .flatMap(_.stride(Int.MaxValue)).isLeft)
  }

  test("finite subset") {
    val r = IndexRange.finite(1, 7, 2)
      .flatMap(_.subset(3, 5))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "3:5:2")
  }

  test("finite subset with truncation") {
    val r = IndexRange.finite(1, 7, 2)
      .flatMap(_.subset(2, 6))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "3:5:2")
  }

  test("finite subset with stride") {
    val r = IndexRange.finite(1, 13, 2)
      .flatMap(_.subset(2, 12, 2))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "3:11:4")
  }

  test("finite subset including start") {
    val r = IndexRange.finite(3, 7, 2)
      .flatMap(_.subset(3, 5))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "3:5:2")
  }

  test("finite subset including end") {
    val r = IndexRange.finite(3, 7, 2)
      .flatMap(_.subset(5, 7))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "5:7:2")
  }

  test("finite subset starting before start") {
    val r = IndexRange.finite(3, 7, 2)
      .flatMap(_.subset(1, 5))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "3:5:2")
  }

  test("finite subset ending after end") {
    val r = IndexRange.finite(3, 7, 2)
      .flatMap(_.subset(5, 9))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "5:7:2")
  }

  test("finite subset outside start should be empty") {
    val r = IndexRange.finite(3, 7, 2)
      .flatMap(_.subset(1, 1))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "0")
  }

  test("finite subset outside end should be empty") {
    val r = IndexRange.finite(3, 7, 2)
      .flatMap(_.subset(9, 9))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "0")
  }

  test("finite chunk") { //first chunk test is 100 time longer?
    val r = IndexRange.finite(1, 7, 2).fold(e => fail(e.getMessage), identity)
    val chunks = r.chunk(2).compile.toList
    assertEquals(chunks.length, 2)
    assertEquals(chunks.last.length.get, 2)
  }

  test("finite chunk with partial chunk") {
    val r = IndexRange.finite(1, 9, 2).fold(e => fail(e.getMessage), identity)
    val chunks = r.chunk(2).compile.toList
    assertEquals(chunks.length, 3)
    assertEquals(chunks.last.length.get, 1)
  }

  test("finite chunk with single big chunk") {
    val r = IndexRange.finite(1, 7, 2).fold(e => fail(e.getMessage), identity)
    val chunks = r.chunk(100).compile.toList
    assertEquals(chunks.length, 1)
    assertEquals(chunks.last.length.get, 4)
  }

  //--- UnlimitedRange ---//

  test("unlimited range has no length") {
    val r = IndexRange.unlimited(1, 2).fold(e => fail(e.getMessage), identity)
    assertEquals(r.length, None)
  }

  test("unlimited range is not empty") {
    val r = IndexRange.unlimited(1, 2).fold(e => fail(e.getMessage), identity)
    assert(!r.isEmpty)
  }

  test("unlimited range is unlimited") {
    val r = IndexRange.unlimited(1, 2).fold(e => fail(e.getMessage), identity)
    assert(r.isUnlimited)
  }

  test("unlimited range stride") {
    val r = IndexRange.unlimited(1, 2)
      .flatMap(_.stride(3))
      .fold(e => fail(e.getMessage), identity)
    assertEquals(r.toString, "1:*:6")
  }

  test("unlimited range subset") {
    val r = IndexRange.unlimited(1, 2)
      .flatMap(_.subset(2, 10, 2))
      .fold(e => fail(e.getMessage), identity)
    assert(!r.isUnlimited) //no longer unlimited
    assertEquals(r.toString, "3:7:4")
  }

  test("unlimited range chunk") {
    val r = IndexRange.unlimited(1, 2)
      .map(_.chunk(10))
      .fold(e => fail(e.getMessage), identity)
      .head.compile.toList.head
    assertEquals(r.toString, "1:19:2")
  }

  //--- EmptyRange ---//

  test("empty range has length 0") {
    assertEquals(EmptyRange.length, Some(0))
  }

  test("empty range is empty") {
    assert(EmptyRange.isEmpty)
  }

  test("empty range is not unlimited") {
    assert(!EmptyRange.isUnlimited)
  }

  test("empty range stride is empty") {
    val r = EmptyRange.stride(2).fold(e => fail(e.getMessage), identity)
    assert(r.isEmpty)
  }

  test("empty range subset is empty") {
    val r = EmptyRange.subset(0, 10, 2).fold(e => fail(e.getMessage), identity)
    assert(r.isEmpty)
  }

  test("empty range toString") {
    assertEquals(EmptyRange.toString, "0")
  }
}

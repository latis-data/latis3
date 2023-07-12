package latis.util

import munit.FunSuite

class SectionSuite extends FunSuite {

  val sectionFromLength: Section =
    Section.fromExpression("10")
      .fold(fail("failed to construct section", _), identity)

  val sectionFromRange: Section =
    Section.fromExpression("0:9")
      .fold(fail("failed to construct section", _), identity)

  val sectionWithOffset: Section =
    Section.fromExpression("1:9")
      .fold(fail("failed to construct section", _), identity)

  val sectionWithStride: Section =
    Section.fromExpression("0:9:2")
      .fold(fail("failed to construct section", _), identity)

  val sectionWithOffsetAndStride: Section =
    Section.fromExpression("1:9:2")
      .fold(fail("failed to construct section", _), identity)

  val unlimitedSection: Section =
    Section.fromExpression("0:*")
      .fold(fail("failed to construct section", _), identity)

  val unlimitedSectionWithStride: Section =
    Section.fromExpression("0:*:2")
      .fold(fail("failed to construct section", _), identity)

  val section2D: Section =
    Section.fromExpression("0:*,0:9")
      .fold(fail("failed to construct section", _), identity)

  //---- Shape ----//

  test("shape of section from length") {
    assertEquals(sectionFromLength.shape, List(10))
  }
  test("shape of section from range") {
    assertEquals(sectionFromRange.shape, List(10))
  }
  test("shape of section with offset") {
    assertEquals(sectionWithOffset.shape, List(9))
  }
  test("shape of section with stride") {
    assertEquals(sectionWithStride.shape, List(5))
  }
  test("shape of section with offset and stride") {
    assertEquals(sectionWithOffsetAndStride.shape, List(5))
  }
  test("shape of unlimited section") {
    assertEquals(unlimitedSection.shape, List(-1))
  }
  test("shape of 2D section") {
    assertEquals(section2D.shape, List(-1, 10))
  }

  //---- Head ----//

  test("head of 2D") {
    assertEquals(section2D.head.shape, List(1, 1))
  }

  test("head of unlimited") {
    //Note that the result has a stride of 1 even though it doesn't matter.
    assertEquals(unlimitedSectionWithStride.head.toString, "0:0:1")
  }

  //---- Stride ----//

  test("stride by dimension") {
    sectionWithStride.strideDimension(0, 2) match {
      case Right(s: Section) => assertEquals(s.toString, "0:8:4")
      case _ => fail("unsupported stride")
    }
  }

  test("stride unlimited dimension") {
    unlimitedSection
      .strideDimension(0,3)
      .flatMap(_.subsetDimension(0, 0, 9))
      .fold(
        fail("failed to construct section", _),
        s => assertEquals(s.toString(), "0:9:3")
      )
  }

  test("one-dimensional stride") {
    sectionWithStride.stride(List(2)) match {
      case Right(s: Section) => assertEquals(s.toString, "0:8:4")
      case _ => fail("unsupported stride")
    }
  }

  test("multi-dimensional stride") {
    section2D.stride(List(2, 3)) match {
      case Right(s: Section) => assertEquals(s.toString, "0:*:2,0:9:3")
      case _ => fail("unsupported stride")
    }
  }

  //---- Subset ----//

  test("subset unlimited section") {
    unlimitedSection.subsetDimension(0, 5, 10) match {
      case Right(s: Section) => assertEquals(s.toString, "5:10:1")
      case _ => fail("unsupported stride")
    }
  }

  test("subset beyond range") {
    sectionWithOffset.subsetDimension(0, 0, 100) match {
      case Right(s: Section) => assertEquals(s, sectionWithOffset)
      case _ => fail("unsupported stride")
    }
  }

  test("subset complex section") {
    sectionWithOffsetAndStride.subsetDimension(0, 4, 7) match {
      case Right(s) => assertEquals(s.toString, "5:7:2")
      case _ => fail("unsupported stride")
    }
  }

  test("subset with stride") {
    sectionWithStride.subsetDimension(0, 2, 8, 2) match {
      case Right(s) => assertEquals(s.toString, "2:6:4")
      case _ => fail("unsupported stride")
    }
  }

  test("subset second dimension") {
    section2D.subsetDimension(1, 3, 7) match {
      case Right(s) => assertEquals(s.toString, "0:*:1,3:7:1")
      case _ => fail("unsupported stride")
    }
  }

  test("subset outside section") {
    val s = sectionWithOffset.subsetDimension(0, 10, 20).fold(e => fail(e.getMessage), identity)
    assert(s.isEmpty)
  }

  test("prevent from > to") {
    assert(sectionWithOffset.subsetDimension(0, 10, -1).isLeft)
  }

  //---- Chunk ----//

  test("chunk") {
    val s = Section.fromExpression("1:9:2,0:2")
      .fold(fail("failed to construct secion", _), identity)

    val ss = s.chunk(4).compile.toList
    assert(ss(0).toString == "1:3:2,0:2:1")
    assert(ss(1).toString == "5:7:2,0:2:1")
    assert(ss(2).toString == "9:9:2,0:2:1")
  }

  //---- Empty ----//

  test("empty section length") {
    assert(Section.empty.length.contains(0L))
  }

  test("empty section rank") {
    assert(Section.empty.rank == 0)
  }

  test("empty section shape") {
    assert(Section.empty.shape.isEmpty)
  }

  test("empty section is empty") {
    assert(Section.empty.isEmpty)
  }

  test("empty section head") {
    assert(Section.empty.head.isEmpty)
  }

  test("empty section range") {
    assert(Section.empty.range(0).isLeft)
  }

  test("empty section strideDimension") {
    val s = Section.empty.strideDimension(0,2).fold(e => fail(e.getMessage), identity)
    assert(s.isEmpty)
  }

  test("empty section stride") {
    val s = Section.empty.stride(List(2,2)).fold(e => fail(e.getMessage), identity)
    assert(s.isEmpty)
  }

  test("empty section chunk") {
    assert(Section.empty.chunk(10).compile.toList.isEmpty)
  }

  test("empty section subsetDimension") {
    val s = Section.empty.subsetDimension(0, 2, 3).fold(e => fail(e.getMessage), identity)
    assert(s.isEmpty)
  }

}

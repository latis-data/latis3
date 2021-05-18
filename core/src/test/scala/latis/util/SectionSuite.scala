package latis.util

import org.scalatest.funsuite.AnyFunSuite

class SectionSuite extends AnyFunSuite {

  val sectionFromLength: Section =
    Section.fromExpression("10").toTry.get
  val sectionFromRange: Section =
    Section.fromExpression("0:9").toTry.get
  val sectionWithOffset: Section =
    Section.fromExpression("1:9").toTry.get
  val sectionWithStride: Section =
    Section.fromExpression("0:9:2").toTry.get
  val sectionWithOffsetAndStride: Section =
    Section.fromExpression("1:9:2").toTry.get
  val unlimitedSection: Section =
    Section.fromExpression("0:*").toTry.get
  val unlimitedSectionWithStride: Section =
    Section.fromExpression("0:*:2").toTry.get
  val section2D: Section =
    Section.fromExpression("0:*,0:9").toTry.get

  //---- Shape ----//

  test("shape of section from length") {
    assert(sectionFromLength.shape == List(10))
  }
  test("shape of section from range") {
    assert(sectionFromRange.shape == List(10))
  }
  test("shape of section with offset") {
    assert(sectionWithOffset.shape == List(9))
  }
  test("shape of section with stride") {
    assert(sectionWithStride.shape == List(5))
  }
  test("shape of section with offset and stride") {
    assert(sectionWithOffsetAndStride.shape == List(5))
  }
  test("shape of unlimited section") {
    assert(unlimitedSection.shape == List(-1))
  }
  test("shape of 2D section") {
    assert(section2D.shape == List(-1, 10))
  }

  //---- Head ----//

  test("head of 2D") {
    section2D.head match {
      case s: Section => assert(s.shape == List(1, 1))
    }
  }

  //---- Stride ----//

  test("stride by dimension") {
    sectionWithStride.strideDimension(0, 2) match {
      case Right(s: Section) => assert(s.shape == List(3))
    }
  }

  test("stride unlimited dimension") {
    for {
      s1 <- unlimitedSection.strideDimension(0,3)
      s2 <- s1.subsetDimension(0, 0, 9)
    } yield {
      assert(s2.shape == List(4))
    }
  }

  //---- Subset ----//

  test("subset unlimited section") {
    unlimitedSection.subsetDimension(0, 5, 10) match {
      case Right(s: Section) => assert(s.shape == List(6))
    }
  }

  test("subset beyond range") {
    sectionWithOffset.subsetDimension(0, 0, 100) match {
      case Right(s: Section) => assert(s.shape == sectionWithOffset.shape)
    }
  }

  test("subset complex section") {
    for {
      s1 <- sectionWithOffsetAndStride.subsetDimension(0, 4, 7)
      s2 <- Section.fromExpression("4:7:2")
    } yield {
      assert(s1 == s2)
    }
  }

  test("subset with stride") {
    for {
      s1 <- sectionWithStride.subsetDimension(0, 2, 8, 2)
      s2 <- Section.fromExpression("2:8:4")
    } yield {
      assert(s1 == s2)
    }
  }

  test("subset second dimension") {
    for {
      s1 <- section2D.subsetDimension(1, 3, 7)
      s2 <- Section.fromExpression("0:*,3:7:1")
    } yield {
      assert(s1 == s2)
    }
  }
}

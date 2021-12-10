package latis.util

import cats.effect.unsafe.implicits.global
import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

class SectionSuite extends AnyFunSuite {

  val sectionFromLength: Section =
    Section.fromExpression("10").value
  val sectionFromRange: Section =
    Section.fromExpression("0:9").value
  val sectionWithOffset: Section =
    Section.fromExpression("1:9").value
  val sectionWithStride: Section =
    Section.fromExpression("0:9:2").value
  val sectionWithOffsetAndStride: Section =
    Section.fromExpression("1:9:2").value
  val unlimitedSection: Section =
    Section.fromExpression("0:*").value
  val unlimitedSectionWithStride: Section =
    Section.fromExpression("0:*:2").value
  val section2D: Section =
    Section.fromExpression("0:*,0:9").value

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
    assert(section2D.head.shape == List(1, 1))
  }

  test("head of unlimited") {
    //Note that the result has a stride of 1 even though it doesn't matter.
    assert(unlimitedSectionWithStride.head.toString == "0:0:1")
  }

  //---- Stride ----//

  test("stride by dimension") {
    inside(sectionWithStride.strideDimension(0, 2)) {
      case Right(s: Section) => assert(s.toString == "0:9:4")
    }
  }

  test("stride unlimited dimension") {
    (for {
      s1 <- unlimitedSection.strideDimension(0,3)
      s2 <- s1.subsetDimension(0, 0, 9)
    } yield {
      assert(s2.toString == "0:9:3")
    }).value
  }

  test("one-dimensional stride") {
    inside(sectionWithStride.stride(List(2))) {
      case Right(s: Section) => assert(s.toString == "0:9:4")
    }
  }

  test("multi-dimensional stride") {
    inside(section2D.stride(List(2, 3))) {
      case Right(s: Section) => assert(s.toString == "0:*:2,0:9:3")
    }
  }

  //---- Subset ----//

  test("subset unlimited section") {
    inside(unlimitedSection.subsetDimension(0, 5, 10)) {
      case Right(s: Section) => assert(s.toString == "5:10:1")
    }
  }

  test("subset beyond range") {
    inside(sectionWithOffset.subsetDimension(0, 0, 100)) {
      case Right(s: Section) => assert(s == sectionWithOffset)
    }
  }

  test("subset complex section") {
    inside(sectionWithOffsetAndStride.subsetDimension(0, 4, 7)) {
      case Right(s) => assert(s.toString == "4:7:2")
    }
  }

  test("subset with stride") {
    inside(sectionWithStride.subsetDimension(0, 2, 8, 2)) {
      case Right(s) => assert(s.toString == "2:8:4")
    }
  }

  test("subset second dimension") {
    inside(section2D.subsetDimension(1, 3, 7)) {
      case Right(s) => assert(s.toString == "0:*:1,3:7:1")
    }
  }

  //---- Chunk ----//

  test("chunk") {
    val s = Section.fromExpression("1:9:2,0:2").value
    s.chunk(4).compile.toList.map { ss =>
      assert(ss(0).toString == "1:3:2,0:2:1")
      assert(ss(1).toString == "5:7:2,0:2:1")
      assert(ss(2).toString == "9:9:2,0:2:1")
    }.unsafeRunSync()
  }
}

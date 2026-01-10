package latis.ops

import munit.FunSuite

import latis.data.*
import latis.data.Data.*
import latis.model.*
import latis.util.Identifier.id

class InterpolationSuite extends FunSuite {

  private val model = Function.from(Scalar(id"x", IntValueType), Scalar(id"y", IntValueType))
    .fold(throw _, identity)
  private val samples = List(
    Sample(DomainData(0), RangeData(0)),
    Sample(DomainData(3), RangeData(3))
  )

  test("Nearest interpolation will round up") {
    val interp = NearestInterpolation()
    val z = interp.interpolate(model, samples, IntValue(2))
      z.fold(_ => fail("Failed to interpolate"), identity) match {
      case Sample(DomainData(d: IntValue), RangeData(r: IntValue)) =>
        assertEquals(d.value, 2)
        assertEquals(r.value, 3)
    }
  }

  test("Floor interpolation") {
    val interp = FloorInterpolation()
    interp.interpolate(model, samples, IntValue(2))
      .fold(_ => fail("Failed to interpolate"), identity) match {
      case Sample(DomainData(d: IntValue), RangeData(r: IntValue)) =>
        assertEquals(d.value, 2)
        assertEquals(r.value, 0)
    }
  }

  test("first sample matches") {
    val interp = NearestInterpolation()
    interp.interpolate(model, samples, IntValue(0))
      .fold(_ => fail("Failed to interpolate"), identity) match {
      case Sample(DomainData(d: IntValue), RangeData(r: IntValue)) =>
        assertEquals(d.value, 0)
        assertEquals(r.value, 0)
    }
  }

  test("last sample matches") {
    val interp = NearestInterpolation()
    interp.interpolate(model, samples, IntValue(3))
      .fold(_ => fail("Failed to interpolate"), identity) match {
      case Sample(DomainData(d: IntValue), RangeData(r: IntValue)) =>
        assertEquals(d.value, 3)
        assertEquals(r.value, 3)
    }
  }

  test("Can't extrapolate") {
    val interp = NearestInterpolation()
    val s = interp.interpolate(model, samples, IntValue(-1))
    assert(s.isLeft)
  }

  test("No interpolation with matching sample") {
    val interp = NoInterpolation()
    interp.interpolate(model, samples, IntValue(0))
      .fold(_ => fail("Failed to interpolate"), identity) match {
      case Sample(DomainData(d: IntValue), RangeData(r: IntValue)) =>
        assertEquals(d.value, 0)
        assertEquals(r.value, 0)
    }
  }

  test("No interpolation fails with no match") {
    val interp = NoInterpolation()
    val s = interp.interpolate(model, samples, IntValue(1))
    assert(s.isLeft)
  }

  test("Floor interpolation with string domain") {
    val model = Function.from(Scalar(id"x", StringValueType), Scalar(id"y", IntValueType))
      .fold(throw _, identity)
    val samples = List(
      Sample(DomainData("A"), RangeData(0)),
      Sample(DomainData("C"), RangeData(3))
    )
    val interp = FloorInterpolation()
    interp.interpolate(model, samples, StringValue("B"))
      .fold(_ => fail("Failed to interpolate"), identity) match {
      case Sample(DomainData(d: StringValue), RangeData(r: IntValue)) =>
        assertEquals(d.value, "B")
        assertEquals(r.value, 0)
    }
  }

  test("Linear interpolation") {
    val model = Function.from(Scalar(id"x", DoubleValueType), Scalar(id"y", DoubleValueType))
      .fold(throw _, identity)
    val samples = List(
      Sample(DomainData(1.0), RangeData(1.0)),
      Sample(DomainData(2.0), RangeData(2.0))
    )
    LinearInterpolation().interpolate(model, samples, DoubleValue(1.5))
      .fold(throw _, identity) match {
      case Sample(DomainData(Number(d)), RangeData(Number(r))) =>
        assertEquals(d, 1.5)
        assertEquals(r, 1.5)
    }
  }

  test("Linear interpolation matches first") {
    val model = Function.from(Scalar(id"x", IntValueType), Scalar(id"y", DoubleValueType))
      .fold(throw _, identity)
    val samples = List(
      Sample(DomainData(1), RangeData(1.0)),
      Sample(DomainData(2), RangeData(2.0))
    )
    LinearInterpolation().interpolate(model, samples, IntValue(1))
      .fold(throw _, identity) match {
      case Sample(DomainData(d: IntValue), RangeData(Number(r))) =>
        assertEquals(d.value, 1)
        assertEquals(r, 1.0)
    }
  }

  test("Linear interpolation matches last") {
    val model = Function.from(Scalar(id"x", IntValueType), Scalar(id"y", DoubleValueType))
      .fold(throw _, identity)
    val samples = List(
      Sample(DomainData(1), RangeData(1.0)),
      Sample(DomainData(2), RangeData(2.0))
    )
    LinearInterpolation().interpolate(model, samples, IntValue(2))
      .fold(throw _, identity) match {
      case Sample(DomainData(d: IntValue), RangeData(Number(r))) =>
        assertEquals(d.value, 2)
        assertEquals(r, 2.0)
    }
  }

}

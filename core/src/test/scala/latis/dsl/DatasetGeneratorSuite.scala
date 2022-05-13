package latis.dsl

import munit.FunSuite

import latis.data._
import latis.data.Data._
import latis.model._

class DatasetGeneratorSuite extends FunSuite {

  test("make a 1D dataset") {
    val xs = Seq(1,2,3)
    val as = Seq("a", "b", "c")
    val bs = Seq(1.1, 2.2, 3.3)

    val ds = DatasetGenerator.generate1DDataset(xs, as, bs)
    assertEquals(ds.model.toString, "_1 -> (a, b)")
    ds.model match {
      case Function(x: Scalar, Tuple(a: Scalar, b: Scalar)) =>
        assertEquals(x.valueType, IntValueType)
        assertEquals(a.valueType, StringValueType)
        assertEquals(b.valueType, DoubleValueType)
      case _ => fail("model is not the correct type of function")
    }
    ds.data.sampleSeq.head match {
      case Sample(DomainData(x: IntValue), RangeData(a: StringValue, b: DoubleValue)) =>
        assertEquals(x.value, 1)
        assertEquals(a.value, "a")
        assertEquals(b.value, 1.1)
      case _ => fail("sample is not of the correct type")
    }
  }

  test("make a 2D dataset") {
    val xs = Seq('a', 'b')
    val ys = Seq(1, 2, 3)
    val as = Seq(Seq("a1", "a2", "a3"), Seq("b1", "b2", "b3"))
    val bs = Seq(Seq(1.1, 2.2, 3.3), Seq(4.4, 5.5, 6.6))

    val ds = DatasetGenerator.generate2DDataset(xs, ys, as, bs)
    assertEquals(ds.model.toString, "(_1, _2) -> (a, b)")
    ds.model match {
      case Function(Tuple(x: Scalar, y: Scalar), Tuple(a: Scalar, b: Scalar)) =>
        assertEquals(x.valueType, CharValueType)
        assertEquals(y.valueType, IntValueType)
        assertEquals(a.valueType, StringValueType)
        assertEquals(b.valueType, DoubleValueType)
      case _ => fail("model is not the correct type of function")
    }
    ds.data.sampleSeq.head match {
      case Sample(DomainData(x: CharValue, y: IntValue), RangeData(a: StringValue, b: DoubleValue)) =>
        assertEquals(x.value, 'a')
        assertEquals(y.value, 1)
        assertEquals(a.value, "a1")
        assertEquals(b.value, 1.1)
      case _ => fail("sample is not of the correct type")
    }
  }

  test("make a 3D dataset") {
    val xs = Seq('a', 'b')
    val ys = Seq(1, 2)
    val zs = Seq(0.1, 0.2)
    val as = Seq(
      Seq(Seq("a1.1", "a1.2"), Seq("a2.1", "a2.2")),
      Seq(Seq("b1.1", "b1.2"), Seq("b2.1", "b2.2"))
    )
    val bs = Seq(
      Seq(Seq(1.1, 2.2), Seq(3.3, 4.4)),
      Seq(Seq(5.5, 6.6), Seq(7.7, 8.8))
    )

    val ds = DatasetGenerator.generate3DDataset(xs, ys, zs, as, bs)
    assertEquals(ds.model.toString, "(_1, _2, _3) -> (a, b)")
    ds.model match {
      case Function(Tuple(x: Scalar, y: Scalar, z: Scalar), Tuple(a: Scalar, b: Scalar)) =>
        assertEquals(x.valueType, CharValueType)
        assertEquals(y.valueType, IntValueType)
        assertEquals(z.valueType, DoubleValueType)
        assertEquals(a.valueType, StringValueType)
        assertEquals(b.valueType, DoubleValueType)
      case _ => fail("model is not the correct type of function")
    }
    // test a couple samples to make sure the order is what we expect
    assertEquals(ds.data.sampleSeq.head, Sample(DomainData('a', 1, 0.1), RangeData("a1.1", 1.1)))
    assertEquals(ds.data.sampleSeq.drop(1).head, Sample(DomainData('a', 1, 0.2), RangeData("a1.2", 2.2)))
    assertEquals(ds.data.sampleSeq.drop(2).head, Sample(DomainData('a', 2, 0.1), RangeData("a2.1", 3.3)))
    assertEquals(ds.data.sampleSeq.drop(4).head, Sample(DomainData('b', 1, 0.1), RangeData("b1.1", 5.5)))
  }

  test("generate a 1D dataset from a String") {
    val ds = DatasetGenerator("a: double -> (b: double, c: boolean)")
    assertEquals(ds.data.sampleSeq.head, Sample(DomainData(0.0), RangeData(0.0, true)))
    assertEquals(ds.data.sampleSeq(1), Sample(DomainData(1.0), RangeData(1.0, false)))
    assertEquals(ds.data.sampleSeq(2), Sample(DomainData(2.0), RangeData(2.0, true)))
  }

  test("generate a 2D dataset from a String") {
    val ds = DatasetGenerator("(a: string, b: int) -> (c: double, d: double)")
    assertEquals(ds.data.sampleSeq.head, Sample(DomainData("a", 0), RangeData(0.0, 1.0)))
    assertEquals(ds.data.sampleSeq(1), Sample(DomainData("a", 1), RangeData(2.0, 3.0)))
    assertEquals(ds.data.sampleSeq(2), Sample(DomainData("a", 2), RangeData(4.0, 5.0)))
    assertEquals(ds.data.sampleSeq(3), Sample(DomainData("b", 0), RangeData(6.0, 7.0)))
    assertEquals(ds.data.sampleSeq(4), Sample(DomainData("b", 1), RangeData(8.0, 9.0)))
    assertEquals(ds.data.sampleSeq(5), Sample(DomainData("b", 2), RangeData(10.0, 11.0)))
  }

  test("generate a 3D dataset from a String") {
    val ds = DatasetGenerator("(a: string, b, c) -> d: double")
    assertEquals(ds.data.sampleSeq.head, Sample(DomainData("a", 0, 0), RangeData(0.0)))
    assertEquals(ds.data.sampleSeq(1), Sample(DomainData("a", 0, 1), RangeData(1.0)))
    assertEquals(ds.data.sampleSeq(4), Sample(DomainData("a", 1, 0), RangeData(4.0)))
    assertEquals(ds.data.sampleSeq(11), Sample(DomainData("a", 2, 3), RangeData(11.0)))
    assertEquals(ds.data.sampleSeq(12), Sample(DomainData("b", 0, 0), RangeData(12.0)))
    assertEquals(ds.data.sampleSeq(23), Sample(DomainData("b", 2, 3), RangeData(23.0)))
  }
}

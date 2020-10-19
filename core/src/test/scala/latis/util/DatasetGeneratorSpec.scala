package latis.util

import atto.Atto._
import atto.ParseResult
import atto.Parser
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.Data._
import latis.data._
import latis.model._

class DatasetGeneratorSpec extends FlatSpec {

  "A DatasetGenerator" should "make a 1D dataset" in {
    val xs = Seq(1,2,3)
    val as = Seq("a", "b", "c")
    val bs = Seq(1.1, 2.2, 3.3)

    val ds = DatasetGenerator.generate1DDataset(xs, as, bs)
    ds.model.toString should be ("_1 -> (a, b)")
    ds.model match {
      case Function(x: Scalar, Tuple(a: Scalar, b: Scalar)) =>
        x.valueType should be (IntValueType)
        a.valueType should be (StringValueType)
        b.valueType should be (DoubleValueType)
    }
    ds.data.sampleSeq.head match {
      case Sample(DomainData(x: IntValue), RangeData(a: StringValue, b: DoubleValue)) =>
        x.value should be (1)
        a.value should be ("a")
        b.value should be (1.1)
    }
  }

  it should "make a 2D dataset" in {
    val xs = Seq('a', 'b')
    val ys = Seq(1, 2, 3)
    val as = Seq(Seq("a1", "a2", "a3"), Seq("b1", "b2", "b3"))
    val bs = Seq(Seq(1.1, 2.2, 3.3), Seq(4.4, 5.5, 6.6))

    val ds = DatasetGenerator.generate2DDataset(xs, ys, as, bs)
    ds.model.toString should be ("(_1, _2) -> (a, b)")
    ds.model match {
      case Function(Tuple(x: Scalar, y: Scalar), Tuple(a: Scalar, b: Scalar)) =>
        x.valueType should be (CharValueType)
        y.valueType should be (IntValueType)
        a.valueType should be (StringValueType)
        b.valueType should be (DoubleValueType)
    }
    ds.data.sampleSeq.head match {
      case Sample(DomainData(x: CharValue, y: IntValue), RangeData(a: StringValue, b: DoubleValue)) =>
        x.value should be ('a')
        y.value should be (1)
        a.value should be ("a1")
        b.value should be (1.1)
    }
  }

  it should "make a 3D dataset" in {
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
    ds.model.toString should be ("(_1, _2, _3) -> (a, b)")
    ds.model match {
      case Function(Tuple(x: Scalar, y: Scalar, z: Scalar), Tuple(a: Scalar, b: Scalar)) =>
        x.valueType should be (CharValueType)
        y.valueType should be (IntValueType)
        z.valueType should be (DoubleValueType)
        a.valueType should be (StringValueType)
        b.valueType should be (DoubleValueType)
    }
    // test a couple samples to make sure the order is what we expect
    ds.data.sampleSeq.head should be (Sample(DomainData('a', 1, 0.1), RangeData("a1.1", 1.1)))
    ds.data.sampleSeq.drop(1).head should be (Sample(DomainData('a', 1, 0.2), RangeData("a1.2", 2.2)))
    ds.data.sampleSeq.drop(2).head should be (Sample(DomainData('a', 2, 0.1), RangeData("a2.1", 3.3)))
    ds.data.sampleSeq.drop(4).head should be (Sample(DomainData('b', 1, 0.1), RangeData("b1.1", 5.5)))
  }

  "fromString" should "generate a 1D dataset" in {
    val ds = DatasetGenerator("double -> (double, boolean)")
    ds.data.sampleSeq(0) should be (Sample(DomainData(0.0), RangeData(0.0, true)))
    ds.data.sampleSeq(1) should be (Sample(DomainData(1.0), RangeData(1.0, false)))
    ds.data.sampleSeq(2) should be (Sample(DomainData(2.0), RangeData(2.0, true)))
  }

  it should "generate a 2D dataset" in {
    val ds = DatasetGenerator("(string, int) -> (double, double)")
    ds.data.sampleSeq(0) should be (Sample(DomainData("a", 0), RangeData(0.0, 1.0)))
    ds.data.sampleSeq(1) should be (Sample(DomainData("a", 1), RangeData(2.0, 3.0)))
    ds.data.sampleSeq(2) should be (Sample(DomainData("a", 2), RangeData(4.0, 5.0)))
    ds.data.sampleSeq(3) should be (Sample(DomainData("b", 0), RangeData(6.0, 7.0)))
    ds.data.sampleSeq(4) should be (Sample(DomainData("b", 1), RangeData(8.0, 9.0)))
    ds.data.sampleSeq(5) should be (Sample(DomainData("b", 2), RangeData(10.0, 11.0)))
  }

  it should "generate a 3D dataset" in {
    val ds = DatasetGenerator("(string, int, int) -> double")
    ds.data.sampleSeq(0)  should be (Sample(DomainData("a", 0, 0), RangeData(0.0)))
    ds.data.sampleSeq(1)  should be (Sample(DomainData("a", 0, 1), RangeData(1.0)))
    ds.data.sampleSeq(4)  should be (Sample(DomainData("a", 1, 0), RangeData(4.0)))
    ds.data.sampleSeq(11) should be (Sample(DomainData("a", 2, 3), RangeData(11.0)))
    ds.data.sampleSeq(12) should be (Sample(DomainData("b", 0, 0), RangeData(12.0)))
    ds.data.sampleSeq(23) should be (Sample(DomainData("b", 2, 3), RangeData(23.0)))
  }
}

class ModelParserSpec extends FlatSpec {

  "modelParser" should "parse scalars" in {
    val testScalar = testParser(modelParser.scalar)(_, _)
    testScalar("double", modelAst.Scalar(DoubleValueType))
    testScalar("int", modelAst.Scalar(IntValueType))
  }

  "modelParser" should "parse tuples" in {
    val testTuple = testParser(modelParser.tuple)(_, _)
    testTuple("(double, int)",
      modelAst.Tuple(List(
        modelAst.Scalar(DoubleValueType),
        modelAst.Scalar(IntValueType)
      ))
    )
  }

  "modelParser" should "parse functions" in {
    val testFunction = testParser(modelParser.function)(_, _)
    testFunction("int -> double",
      modelAst.Function(
        modelAst.Scalar(IntValueType),
        modelAst.Scalar(DoubleValueType)
      )
    )
    testFunction("(string, int) -> (double, double)",
      modelAst.Function(
        modelAst.Tuple(List(
          modelAst.Scalar(StringValueType),
          modelAst.Scalar(IntValueType)
        )),
        modelAst.Tuple(List(
          modelAst.Scalar(DoubleValueType),
          modelAst.Scalar(DoubleValueType)
        ))
      )
    )
    testFunction("int -> double -> double",
      modelAst.Function(
        modelAst.Scalar(IntValueType),
        modelAst.Function(
          modelAst.Scalar(DoubleValueType),
          modelAst.Scalar(DoubleValueType)
        )
      )
    )
    testFunction("int -> (double -> double)",
      modelAst.Function(
        modelAst.Scalar(IntValueType),
        modelAst.Function(
          modelAst.Scalar(DoubleValueType),
          modelAst.Scalar(DoubleValueType)
        )
      )
    )
  }

  /**
   * Partially apply with a parser to get a function that takes the string you
   * want to parse and the thing you expect to get back
   */
  private def testParser[A](p: Parser[A])(s: String, d: A): Unit = p.parseOnly(s) match {
    case ParseResult.Done(_, result) => result should be (d)
    case ParseResult.Fail(_, _, m) => fail(s"$m in $s")
    // parseOnly will never return anything but Done or Fail, but the types don't
    // know that so we get a warning without the following line.
    case _ => fail(s"failed to parse $s")
  }
}
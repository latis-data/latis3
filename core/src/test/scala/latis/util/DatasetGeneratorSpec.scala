package latis.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data._
import latis.data.Data._
import latis.model._
import latis.output.TextWriter

class DatasetGeneratorSpec extends FlatSpec {

  "A DatasetGenerator" should "make a 1D dataset" in {
    val xs = Seq(1,2,3)
    val as = Seq("a", "b", "c")
    val bs = Seq(1.1, 2.2, 3.3)

    val ds = DatasetGenerator.generate1DDataset(xs, as, bs)
    //TextWriter().write(ds)
    ds.model.toString should be ("_1 -> (a, b)")
    ds.model match {
      case Function(x: Scalar, Tuple(a: Scalar, b: Scalar)) =>
        x.valueType should be (IntValueType)
        a.valueType should be (StringValueType)
        b.valueType should be (DoubleValueType)
    }
    ds.data.sampleSeq.head match {
      //case Sample(DomainData(Integer(x)), RangeData(Text(a), Number(b))) =>
      case Sample(DomainData(x: IntValue), RangeData(a: StringValue, b: DoubleValue)) =>
        x.value should be (1)
        a.value should be ("a")
        b.value should be (1.1)
    }
  }

  it should "make a 2D dataset" in {
    val xs = Seq('a', 'b')
    val ys = Seq(1.toByte, 2.toByte, 3.toByte)
    val as = Seq(Seq("a1", "a2", "a3"), Seq("b1", "b2", "b3"))
    val bs = Seq(Seq(1.1f, 2.2f, 3.3f), Seq(4.4f, 5.5f, 6.6f))

    val ds = DatasetGenerator.generate2DDataset(xs, ys, as, bs)
    //TextWriter().write(ds)
    ds.model.toString should be ("(_1, _2) -> (a, b)")
    ds.model match {
      case Function(Tuple(x: Scalar, y: Scalar), Tuple(a: Scalar, b: Scalar)) =>
        x.valueType should be (CharValueType)
        y.valueType should be (ByteValueType)
        a.valueType should be (StringValueType)
        b.valueType should be (FloatValueType)
    }
    ds.data.sampleSeq.head match {
      //case Sample(DomainData(Integer(x)), RangeData(Text(a), Number(b))) =>
      case Sample(DomainData(x: CharValue, y: ByteValue), RangeData(a: StringValue, b: FloatValue)) =>
        x.value should be ('a')
        y.value should be (1.toByte)
        a.value should be ("a1")
        b.value should be (1.1f)
    }
  }
}

package latis.ops

import latis.data._
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model._
import latis.output.TextWriter
import latis.time.Time
import latis.util.StreamUtils

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class TimeTupleToTimeSpec extends FlatSpec {
  
  def mockDataset: MemoizedDataset = {
    val metadata: Metadata = Metadata("MockDataset" + ("title" -> "Mock Dataset"))
    val model: DataType = Function(
      Tuple(Metadata("time"),
        Scalar(Metadata("year")  + ("type" -> "string") + ("units" -> "yyyy")),
        Scalar(Metadata("month") + ("type" -> "string") + ("units" -> "MM")),
        Scalar(Metadata("day")   + ("type" -> "string") + ("units" -> "dd")),
      ),
      Scalar(Metadata("flux") + ("type" -> "int"))
    )
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(Data.IntValue(1945), Data.IntValue(1), Data.IntValue(1)), RangeData(10)),
        Sample(DomainData(Data.IntValue(1945), Data.IntValue(1), Data.IntValue(2)), RangeData(20)),
        Sample(DomainData(Data.IntValue(1945), Data.IntValue(1), Data.IntValue(3)), RangeData(30)),
        Sample(DomainData(Data.IntValue(1945), Data.IntValue(1), Data.IntValue(4)), RangeData(40)),
        Sample(DomainData(Data.IntValue(1945), Data.IntValue(1), Data.IntValue(5)), RangeData(50))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  def mockDataset2: MemoizedDataset = {
    val metadata: Metadata = Metadata("MockDataset2" + ("title" -> "Mock Dataset 2"))
    val model: DataType = Function(
      Tuple(
        Scalar(Metadata("a") + ("type" -> "int")),
        Tuple(Metadata("time"),
          Scalar(Metadata("year")  + ("type" -> "string") + ("units" -> "yyyy")),
          Scalar(Metadata("month") + ("type" -> "string") + ("units" -> "MM"))
        )
      ),
      Scalar(Metadata("flux") + ("type" -> "int"))
    )
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(Data.IntValue(1), Data.IntValue(1945), Data.IntValue(1)), RangeData(10)),
        Sample(DomainData(Data.IntValue(2), Data.IntValue(1945), Data.IntValue(2)), RangeData(20)),
        Sample(DomainData(Data.IntValue(3), Data.IntValue(1945), Data.IntValue(3)), RangeData(30)),
        Sample(DomainData(Data.IntValue(4), Data.IntValue(1945), Data.IntValue(4)), RangeData(40)),
        Sample(DomainData(Data.IntValue(5), Data.IntValue(1945), Data.IntValue(5)), RangeData(50))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  "The TimeTupleToTime operation" should "convert a time tuple to a time scalar" in {
    val ds = mockDataset.withOperation(TimeTupleToTime())

    //TextWriter().write(ds)

    ds.model match {
      case Function(t: Time, _: Scalar) =>
        t.units should be ("yyyy MM dd")
    }

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Text(time)), RangeData(Number(f))) =>
        time should be ("1945 1 1")
        f should be (10)
    }
  }

  it should "convert a nested time tuple to a time scalar" in {
    val ds = mockDataset2.withOperation(TimeTupleToTime())

    //TextWriter().write(ds)

    ds.model match {
      case Function(Tuple(es @ _*), _: Scalar) => es(1) match {
        case t: Time => t.units should be ("yyyy MM")
      }
    }

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(a), Text(time)), RangeData(Number(f))) =>
        a should be (1)
        time should be ("1945 1")
        f should be (10)
    }
  }
  
}

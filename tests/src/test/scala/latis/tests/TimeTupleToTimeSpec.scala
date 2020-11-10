package latis.tests

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data._
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model._
import latis.ops.TimeTupleToTime
import latis.time.Time
import latis.util.Identifier.IdentifierStringContext
import latis.util.StreamUtils

class TimeTupleToTimeSpec extends FlatSpec {
  
  def mockDataset: MemoizedDataset = {
    val metadata: Metadata = Metadata(id"MockDataset") + ("title" -> "Mock Dataset")
    val model: DataType = Function(
      Tuple(Metadata(id"time"),
        Scalar(Metadata(id"year")  + ("type" -> "string") + ("units" -> "yyyy")),
        Scalar(Metadata(id"month") + ("type" -> "string") + ("units" -> "MM")),
        Scalar(Metadata(id"day")   + ("type" -> "string") + ("units" -> "dd")),
      ),
      Scalar(Metadata(id"flux") + ("type" -> "int"))
    )
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(1945, 1, 1), RangeData(10)),
        Sample(DomainData(1945, 1, 2), RangeData(20)),
        Sample(DomainData(1945, 1, 3), RangeData(30)),
        Sample(DomainData(1945, 1, 4), RangeData(40)),
        Sample(DomainData(1945, 1, 5), RangeData(50))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  def mockDataset2: MemoizedDataset = {
    val metadata: Metadata = Metadata(id"MockDataset2") + ("title" -> "Mock Dataset 2")
    val model: DataType = Function(
      Tuple(
        Scalar(Metadata(id"a") + ("type" -> "int")),
        Tuple(Metadata(id"time"),
          Scalar(Metadata(id"year")  + ("type" -> "string") + ("units" -> "yyyy")),
          Scalar(Metadata(id"month") + ("type" -> "string") + ("units" -> "MM"))
        )
      ),
      Scalar(Metadata(id"flux") + ("type" -> "int"))
    )
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(1, 1945, 1), RangeData(10)),
        Sample(DomainData(2, 1945, 2), RangeData(20)),
        Sample(DomainData(3, 1945, 3), RangeData(30)),
        Sample(DomainData(4, 1945, 4), RangeData(40)),
        Sample(DomainData(5, 1945, 5), RangeData(50))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  def mockDataset3: MemoizedDataset = {
    val metadata: Metadata = Metadata(id"MockDataset3") + ("title" -> "Mock Dataset 3")
    val model: DataType = Function(
      Scalar(Metadata(id"a") + ("type" -> "int")),
      Tuple(Metadata(id"time"),
        Scalar(Metadata(id"year")  + ("type" -> "string") + ("units" -> "yyyy")),
        Scalar(Metadata(id"month") + ("type" -> "string") + ("units" -> "MM"))
      )
    )
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(10), RangeData(TupleData(1945, 1))),
        Sample(DomainData(20), RangeData(TupleData(1945, 2))),
        Sample(DomainData(30), RangeData(TupleData(1945, 3))),
        Sample(DomainData(40), RangeData(TupleData(1945, 4))),
        Sample(DomainData(50), RangeData(TupleData(1945, 5)))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  "The TimeTupleToTime operation" should "convert a time tuple to a time scalar" in {
    val ds = mockDataset.withOperation(TimeTupleToTime())

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

  it should "convert a time tuple in the range to a time scalar" in {
    val ds = mockDataset3.withOperation(TimeTupleToTime())

    ds.model match {
      case Function(_: Scalar, t: Time) => t.units should be ("yyyy MM")
    }

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(a)), RangeData(Text(time))) =>
        a should be (10)
        time should be ("1945 1")
    }
  }
  
}

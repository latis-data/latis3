package latis.ops

import cats.syntax.all.*
import munit.CatsEffectSuite

import latis.data.*
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.*
import latis.time.Time
import latis.util.Identifier.*

class TimeTupleToTimeSuite extends CatsEffectSuite {

  def mockDataset: MemoizedDataset = {
    val metadata: Metadata = Metadata(id"MockDataset") + ("title" -> "Mock Dataset")
    val model: DataType = (
      (
        Scalar.fromMetadata(Metadata(id"year")  + ("type" -> "string") + ("units" -> "yyyy")),
        Scalar.fromMetadata(Metadata(id"month") + ("type" -> "string") + ("units" -> "MM")),
        Scalar.fromMetadata(Metadata(id"day")   + ("type" -> "string") + ("units" -> "dd"))
      ).flatMapN(Tuple.fromElements(id"time", _, _, _)),
      Scalar(id"flux", IntValueType).asRight
    ).flatMapN(Function.from).fold(fail("failed to construct model", _), identity)
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
    val model: DataType = (
      (
        Scalar(id"a", IntValueType).asRight,
        (
          Scalar.fromMetadata(Metadata(id"year")  + ("type" -> "string") + ("units" -> "yyyy")),
          Scalar.fromMetadata(Metadata(id"month") + ("type" -> "string") + ("units" -> "MM"))
        ).flatMapN(Tuple.fromElements(id"time", _, _))
      ).flatMapN(Tuple.fromElements(_, _)),
      Scalar(id"flux", IntValueType).asRight
    ).flatMapN(Function.from).fold(fail("failed to construct model", _), identity)
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
    val model: DataType = (
      Scalar(id"a", IntValueType).asRight,
      (
        Scalar.fromMetadata(Metadata(id"year")  + ("type" -> "string") + ("units" -> "yyyy")),
        Scalar.fromMetadata(Metadata(id"month") + ("type" -> "string") + ("units" -> "MM"))
      ).flatMapN(Tuple.fromElements(id"time", _, _))
    ).flatMapN(Function.from).fold(fail("failed to construct model", _), identity)
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

  test("convert a time tuple to a time scalar") {
    val ds = mockDataset.withOperation(TimeTupleToTime())

    ds.model match {
      case Function(t: Time, _: Scalar) =>
        assertEquals(t.units, Option("yyyy MM dd"))
      case _ => fail("unexpected model")
    }

    ds.samples.take(1).compile.lastOrError.map {
      case Sample(DomainData(Text(time)), RangeData(Number(f))) =>
        assertEquals(time, "1945 1 1")
        assertEquals(f, 10.0)
      case _ => fail("unexpected sample")
    }
  }

  test("convert a nested time tuple to a time scalar") {
    val ds = mockDataset2.withOperation(TimeTupleToTime())

    ds.model match {
      case Function(Tuple(_, t: Time, _ @ _*), _: Scalar) =>
        assertEquals(t.units, Option("yyyy MM"))
      case _ => fail("unexpected model")
    }

    ds.samples.take(1).compile.lastOrError.map {
      case Sample(DomainData(Number(a), Text(time)), RangeData(Number(f))) =>
        assertEquals(a, 1.0)
        assertEquals(time, "1945 1")
        assertEquals(f, 10.0)
      case _ => fail("unexpected sample")
    }
  }

  test("convert a time tuple in the range to a time scalar") {
    val ds = mockDataset3.withOperation(TimeTupleToTime())

    ds.model match {
      case Function(_: Scalar, t: Time) =>
        assertEquals(t.units, Option("yyyy MM"))
      case _ => fail("unexpected model")
    }

    ds.samples.take(1).compile.lastOrError.map {
      case Sample(DomainData(Number(a)), RangeData(Text(time))) =>
        assertEquals(a, 10.0)
        assertEquals(time, "1945 1")
      case _ => fail("unexpected sample")
    }
  }
}

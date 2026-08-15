package latis.ops

import cats.syntax.all.*
import munit.CatsEffectSuite

import latis.data.*
import latis.data.Data.*
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.*
import latis.util.Identifier.*

class ReplaceMissingSuite extends CatsEffectSuite {

  def mockDataset: MemoizedDataset = {
    val metadata: Metadata = Metadata(id"MockDataset")
    val model: DataType = (
      Scalar.fromMetadata(Metadata(id"time") + ("type" -> "int")),
      Scalar.fromMetadata(Metadata(id"band_irradiance") + ("type" -> "int") + ("missingValue" -> "-1"))
    ).flatMapN(Function.from).fold(fail("Failed to construct model", _), identity)
    
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(1955001), RangeData(24)),
        Sample(DomainData(2004322), RangeData(35)),
        Sample(DomainData(2025120), RangeData(-1)),
        Sample(DomainData(2026102), RangeData(92))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  def mockDataset1: MemoizedDataset = {
    val metadata: Metadata = Metadata(id"MockDataset1")
    val model: DataType = (
      Scalar.fromMetadata(Metadata(id"time") + ("type" -> "int")),
      Scalar.fromMetadata(Metadata(id"band_irradiance") + ("type" -> "double") + ("missingValue" -> "NaN"))
    ).flatMapN(Function.from).fold(fail("Failed to construct model", _), identity)
    
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(1955001), RangeData(24.0)),
        Sample(DomainData(2004322), RangeData(35.0)),
        Sample(DomainData(2025120), RangeData(Double.NaN)),
        Sample(DomainData(2026102), RangeData(92.0))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  def mockDataset2: MemoizedDataset = {
    val metadata: Metadata = Metadata(id"MockDataset2")
    val model: DataType = (
      Scalar.fromMetadata(Metadata(id"time") + ("type" -> "int")),
      Scalar.fromMetadata(Metadata(id"band_irradiance") + ("type" -> "double") + ("missingValue" -> "-1.0"))
    ).flatMapN(Function.from).fold(fail("Failed to construct model", _), identity)
    
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(1955001), RangeData(24.5)),
        Sample(DomainData(2004322), RangeData(35.2)),
        Sample(DomainData(2025120), RangeData(92.6)),
        Sample(DomainData(2026102), RangeData(-1.0)),
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  def mockDataset3: MemoizedDataset = {
    val metadata: Metadata = Metadata(id"MockDataset3")
    val model: DataType = (
      Scalar.fromMetadata(Metadata(id"time") + ("type" -> "int")),
      Scalar.fromMetadata(Metadata(id"band_irradiance") + ("type" -> "double") + ("fillValue" -> "-1.0"))
    ).flatMapN(Function.from).fold(fail("Failed to construct model", _), identity)
    
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(1955001), RangeData(24.6)),
        Sample(DomainData(2004322), RangeData(35.1)),
        Sample(DomainData(2025120), RangeData(-1.0)),
        Sample(DomainData(2026102), RangeData(92.9))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  def mockDataset4: MemoizedDataset = {
    val metadata: Metadata = Metadata(id"MockDataset4")
    val model: DataType = (
      Scalar.fromMetadata(Metadata(id"time") + ("type" -> "int")),
      Scalar.fromMetadata(Metadata(id"band_irradiance") + ("type" -> "double"))
    ).flatMapN(Function.from).fold(fail("Failed to construct model", _), identity)
    
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(1955001), RangeData(24.6)),
        Sample(DomainData(2004322), RangeData(35.1)),
        Sample(DomainData(2025120), RangeData(54.3)),
        Sample(DomainData(2026102), RangeData(92.9))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  test("replace missing values in range of simple function using fill value") {
    val ds = mockDataset.withOperation(ReplaceMissing(id"band_irradiance", "-99"))

    ds.model match {
      case Function(_, r: Scalar) =>
        assertEquals(r.missingValue, Some(IntValue(-99)))
        assertEquals(r.isFillable, false)
      case _ => fail("unexpected model")
    }

    ds.samples.compile.toList.assertEquals(
      List(
        Sample(List(1955001), List(24)),
        Sample(List(2004322), List(35)),
        Sample(List(2025120), List(-99)),
        Sample(List(2026102), List(92))
      )
    )
  }

  test("replace NaNs in range of simple function") {
    val ds = mockDataset1.withOperation(ReplaceMissing(id"band_irradiance", "-99.0"))

    ds.model match {
      case Function(_, r: Scalar) =>
        assertEquals(r.missingValue, Some(DoubleValue(-99.0)))
        assertEquals(r.isFillable, false)
      case _ => fail("unexpected model")
    }

    ds.samples.compile.toList.assertEquals(
      List(
        Sample(List(1955001), List(24.0)),
        Sample(List(2004322), List(35.0)),
        Sample(List(2025120), List(-99.0)),
        Sample(List(2026102), List(92.0))
      )
    )
  }

  test("replace missing values with NaN in range of simple function") {
    val ds = mockDataset2.withOperation(ReplaceMissing(id"band_irradiance", "NaN"))

    ds.model match {
      case Function(_, r: Scalar) =>
        r.missingValue match {
          case Some(d: DoubleValue) =>
            assertEquals(d.asDouble.isNaN(), true)
          case _ => 
            fail("missing value should have been replaced with NaN")
        }
        assertEquals(r.isFillable, false)
      case _ => fail("unexpected model")
    }

    val lst = ds.samples.compile.toList

    lst.map(_.take(3)).assertEquals(
      List(
        Sample(List(1955001), List(24.5)),
        Sample(List(2004322), List(35.2)),
        Sample(List(2025120), List(92.6))
      )
    )
    lst.map { _.last match {
      case Sample(l) =>
        assertEquals(l(0), List(IntValue(2026102)))

        l(1)(0) match {
          case d: DoubleValue =>
            assertEquals(d.asDouble.isNaN(), true)
          case _ =>
            fail("missing value should have been replaced with NaN")
        }
    }}
  }

  test("replace missing values in range of simple function using fillValue metadata") {
    val ds = mockDataset3.withOperation(ReplaceMissing(id"band_irradiance", "0.0"))

    ds.model match {
      case Function(_, r: Scalar) =>
        assertEquals(r.missingValue, Some(DoubleValue(0.0)))
        assertEquals(r.isFillable, false)
      case _ => fail("unexpected model")
    }

    ds.samples.compile.toList.assertEquals(
      List(
        Sample(List(1955001), List(24.6)),
        Sample(List(2004322), List(35.1)),
        Sample(List(2025120), List(0.0)),
        Sample(List(2026102), List(92.9))
      )
    )
  }

  test("missing fillValue and missingValue should be a no-op") {
    val ds = mockDataset4.withOperation(ReplaceMissing(id"band_irradiance", "0.0"))

    ds.model match {
      case Function(_, r: Scalar) =>
        assertEquals(r.missingValue, None)
        assertEquals(r.isFillable, false)
      case _ => fail("unexpected model")
    }

    ds.samples.compile.toList.assertEquals(
      List(
        Sample(List(1955001), List(24.6)),
        Sample(List(2004322), List(35.1)),
        Sample(List(2025120), List(54.3)),
        Sample(List(2026102), List(92.9))
      )
    )
  }
  
}

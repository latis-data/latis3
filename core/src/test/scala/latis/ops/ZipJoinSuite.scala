package latis.ops

import cats.effect.*
import fs2.Stream
import munit.CatsEffectSuite

import latis.data.*
import latis.dataset.*
import latis.dsl.*
import latis.metadata.Metadata
import latis.util.Identifier.id

class ZipJoinSuite extends CatsEffectSuite {

  private lazy val ds1 = {
    val metadata = Metadata(id"test1")
    val model = ModelParser.unsafeParse("x: int -> a: double")
    val samples = Stream.range[IO, Int](0, 1000000).map { n =>
      Sample(DomainData(n), RangeData(n.toDouble))
    }
    //val samples = List(
    //  Sample(DomainData(1), RangeData(1.2)),
    //  Sample(DomainData(2), RangeData(2.4)),
    //  Sample(DomainData(3), RangeData(3.6))
    //)
    new TappedDataset(metadata, model, StreamFunction(samples))
  }

  private lazy val ds2 = {
    val metadata = Metadata(id"test2")
    val model = ModelParser.unsafeParse("x: int -> b: double")
    val samples = Stream.range[IO, Int](0, 1000000).map { n =>
      Sample(DomainData(n), RangeData(n.toDouble))
    }
    //val samples = List(
    //  Sample(DomainData(2), RangeData(2.4)),
    //  Sample(DomainData(4), RangeData(4.8))
    //)
    new TappedDataset(metadata, model, StreamFunction(samples))
  }

  test("zip join".ignore) { //21s
    val ds = (ZipJoin().combine(ds1, ds2.project("b"))).fold(throw _, identity)
    ds.show()
    //match {
    //  case Right(ds) => ds.samples.map {
    //    case Sample(_, RangeData(_, d: Data.DoubleValue)) => d.value
    //  }
    //  case _ => fail("Failed to zip")
    //}).compile.toList.map { ds =>
    //  assertEquals(ds, List(2.4, 4.8))
    //}
  }

  test("horizontal join".ignore) { //20s
    val ds = (HorizontalJoin(HorizontalJoinType.Inner)
      .combine(ds1, ds2.project("b"))).fold(throw _, identity)
    ds.show()
  }
}

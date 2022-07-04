package latis.input

import munit.CatsEffectSuite

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.dataset.AdaptedDataset
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier._
import latis.util.NetUtils.resolveUri

class TextAdapterSuite extends CatsEffectSuite {

  test("read text data") {
    val ds = resolveUri("data/data.txt").map { uri =>
      val metadata = Metadata(id"data")
      val model: DataType = ModelParser.unsafeParse("a: int -> (b: int, c: double, d: string)")
      val config = new TextAdapter.Config()
      val adapter = new TextAdapter(model, config)
      new AdaptedDataset(metadata, model, adapter, uri)
    }.fold(fail("failed to make dataset", _), identity)

    val result = ds.samples.compile.toList
    val expected = List(
      Sample(DomainData(0), RangeData(1, 1.1, "a")),
      Sample(DomainData(1), RangeData(2, 2.2, "b")),
      Sample(DomainData(2), RangeData(4, 3.3, "c")),
    )
    result.assertEquals(expected)
  }
}

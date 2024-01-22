package latis.input

import munit.CatsEffectSuite

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.dataset.AdaptedDataset
import latis.dsl.ModelParser
import latis.input
import latis.metadata.Metadata
import latis.model.DataType
import latis.util.Identifier.*
import latis.util.NetUtils.resolveUri

class MatrixTextAdapterSuite extends CatsEffectSuite {

  val ds = resolveUri("data/matrixData.txt").map { uri =>
    val metadata = Metadata(id"matrixData")
    val model: DataType = ModelParser.unsafeParse("(row: int, col: int) -> v: double")
    val config = new input.TextAdapter.Config(("delimiter", ","))
    val adapter = new MatrixTextAdapter(model, config)
    new AdaptedDataset(metadata, model, adapter, uri)
  }.fold(fail("failed to create dataset", _), identity)

  test("read matix data") {
    val result = ds.samples.compile.toList
    val expected = List(
      Sample(DomainData(0, 0), RangeData(5.8e-03)),
      Sample(DomainData(0, 1), RangeData(5.4e03)),
      Sample(DomainData(1, 0), RangeData(5.8)),
      Sample(DomainData(1, 1), RangeData(-5.4))
    )

    result.assertEquals(expected)
  }

}

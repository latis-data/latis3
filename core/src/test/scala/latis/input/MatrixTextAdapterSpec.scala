package latis.input

import java.net.URI

import cats.effect.unsafe.implicits.global
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.dataset.AdaptedDataset
import latis.dsl.ModelParser
import latis.input
import latis.metadata.Metadata
import latis.model.DataType
import latis.util.Identifier.IdentifierStringContext
import latis.util.NetUtils.resolveUri

class MatrixTextAdapterSpec extends AnyFlatSpec {

  val ds = {
    val metadata = Metadata(id"matrixData")
    val model: DataType = ModelParser.unsafeParse("(row: int, col: int) -> v: double")
    val config = new input.TextAdapter.Config(("delimiter", ","))
    val adapter = new MatrixTextAdapter(model, config)
    val uri: URI = resolveUri("data/matrixData.txt").value
    new AdaptedDataset(metadata, model, adapter, uri)
  }

  "A MatrixTextAdapter" should "read matix data" in {
    val result = ds.samples.compile.toList.unsafeRunSync()
    val expected = List(
      Sample(DomainData(0, 0), RangeData(5.8e-03)),
      Sample(DomainData(0, 1), RangeData(5.4e03)),
      Sample(DomainData(1, 0), RangeData(5.8)),
      Sample(DomainData(1, 1), RangeData(-5.4))
    )
    result should be (expected)
  }

}

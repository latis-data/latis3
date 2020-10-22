package latis.input

import java.net.URI

import latis.metadata.Metadata
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.model.Tuple
import latis.data.{DomainData, RangeData, Sample}
import latis.dataset.AdaptedDataset
import latis.input
import latis.util.NetUtils.resolveUri
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.util.Identifier.IdentifierStringContext

class MatrixTextAdapterSpec extends FlatSpec {

  val ds = {
    val metadata = Metadata(id"matrixData")
    val model: DataType = Function(
      Tuple(
        Scalar(Metadata("id" -> "row", "type" -> "int")),
        Scalar(Metadata("id" -> "column", "type" -> "int"))
      ),
      Scalar(Metadata("id" -> "v", "type" -> "double"))
    )
    val config = new input.TextAdapter.Config(("delimiter", ","))
    val adapter = new MatrixTextAdapter(model, config)
    val uri: URI = resolveUri("data/matrixData.txt").right.get
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

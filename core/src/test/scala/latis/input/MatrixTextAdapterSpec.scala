package latis.input

import java.net.URI

import latis.metadata.Metadata
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.model.Tuple
import latis.data.{DomainData, RangeData, Sample}
import latis.util.FileUtils.resolveUri
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class MatrixTextAdapterSpec extends FlatSpec {

  val reader = new AdaptedDatasetReader {
    def uri: URI = resolveUri("core/src/test/resources/data/matrixData.txt").get
    def model: DataType = Function(
      Tuple(
        Scalar(Metadata("id" -> "row", "type" -> "int")),
        Scalar(Metadata("id" -> "column", "type" -> "int"))
      ),
      Scalar(Metadata("id" -> "v", "type" -> "double"))
    )
    val config = new TextAdapter.Config(("delimiter", ","))

    def adapter = new MatrixTextAdapter(model, config)
  }
  val ds = reader.getDataset

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

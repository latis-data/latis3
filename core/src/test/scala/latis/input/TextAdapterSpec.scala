package latis.input

import latis.metadata.Metadata
import latis.util.FileUtils.resolveUri
import latis.model._
import java.net.URI

import latis.data.{DomainData, RangeData, Sample}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class TextAdapterSpec extends FlatSpec {

  "A TextAdapter" should "read text data" in {
    val reader = new AdaptedDatasetReader {
      def uri: URI = resolveUri("core/src/test/resources/data/data.txt").get
      def model: DataType = Function(
        Scalar(Metadata("id" -> "a", "type" -> "int")),
        Tuple(
          Scalar(Metadata("id" -> "b", "type" -> "int")),
          Scalar(Metadata("id" -> "c", "type" -> "double")),
          Scalar(Metadata("id" -> "d", "type" -> "string"))
        )
      )
      val config = new TextAdapter.Config()
      def adapter = new TextAdapter(model, config)
    }

    val ds = reader.getDataset
    val result = ds.samples.compile.toList.unsafeRunSync()
    val expected = List(
      Sample(DomainData(0), RangeData(1, 1.1, "a")),
      Sample(DomainData(1), RangeData(2, 2.2, "b")),
      Sample(DomainData(2), RangeData(4, 3.3, "c")),
    )
    result should be (expected)
  }
}

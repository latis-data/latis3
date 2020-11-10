package latis.tests

import java.net.URI

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.dataset.AdaptedDataset
import latis.input.TextAdapter
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier.IdentifierStringContext
import latis.util.NetUtils.resolveUri

class TextAdapterSpec extends FlatSpec {

  "A TextAdapter" should "read text data" in {
    val ds = {
      def uri: URI = resolveUri("data/data.txt").right.get
      val metadata = Metadata(id"data")
      val model: DataType = Function(
        Scalar(Metadata("id" -> "a", "type" -> "int")),
        Tuple(
          Scalar(Metadata("id" -> "b", "type" -> "int")),
          Scalar(Metadata("id" -> "c", "type" -> "double")),
          Scalar(Metadata("id" -> "d", "type" -> "string"))
        )
      )
      val config = new TextAdapter.Config()
      val adapter = new TextAdapter(model, config)
      new AdaptedDataset(metadata, model, adapter, uri)
    }

    val result = ds.samples.compile.toList.unsafeRunSync()
    val expected = List(
      Sample(DomainData(0), RangeData(1, 1.1, "a")),
      Sample(DomainData(1), RangeData(2, 2.2, "b")),
      Sample(DomainData(2), RangeData(4, 3.3, "c")),
    )
    result should be (expected)
  }
}

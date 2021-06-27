package latis.input

import latis.metadata.Metadata
import latis.util.NetUtils.resolveUri
import latis.model._
import java.net.URI

import latis.data.{DomainData, RangeData, Sample}
import latis.dataset.AdaptedDataset
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.util.Identifier.IdentifierStringContext

class TextAdapterSpec extends AnyFlatSpec {

  "A TextAdapter" should "read text data" in {
    val ds = {
      def uri: URI = resolveUri("data/data.txt").toTry.get
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

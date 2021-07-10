package latis.output

import cats.effect.unsafe.implicits.global
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.Function
import latis.model.IntValueType
import latis.model.Scalar
import latis.util.Identifier.IdentifierStringContext

final class MetadataEncoderSpec extends AnyFlatSpec {

  private val dataset: Dataset = {
    val metadata = Metadata(id"dataset")

    val model = Function.from(id"function",
      Scalar(id"a", IntValueType),
      Scalar(id"b", IntValueType),
    ).value

    val data = SampledFunction(Seq.empty)

    new MemoizedDataset(metadata, model, data)
  }

  "A metadata encoder" should "encode the ID of the dataset being encoded" in {
    val md = (new MetadataEncoder).encode(dataset).compile.lastOrError.unsafeRunSync()

    val cursor = md.hcursor
    cursor.get[String]("id").getOrElse {
      fail("Missing dataset ID")
    } should equal ("dataset")
    cursor.get[String]("model").getOrElse {
      fail("Missing model")
    } should equal ("function: a -> b")

    val variables = cursor.downField("variables")
    variables.downN(0).get[String]("id").getOrElse {
      fail("Missing variable ID")
    } should equal ("a")
    variables.downN(0).get[String]("type").getOrElse {
      fail("Missing variable type")
    } should equal ("int")
    variables.downN(1).get[String]("id").getOrElse {
      fail("Missing variable ID")
    } should equal ("b")
    variables.downN(1).get[String]("type").getOrElse {
      fail("Missing variable type")
    } should equal ("int")
  }
}

package latis.output

import munit.CatsEffectSuite

import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.Function
import latis.model.IntValueType
import latis.model.Scalar
import latis.util.Identifier.IdentifierStringContext

final class MetadataEncoderSuite extends CatsEffectSuite {

  private lazy val dataset: Dataset = {
    val metadata = Metadata(id"dataset")

    val model = Function.from(id"function",
      Scalar(id"a", IntValueType),
      Scalar(id"b", IntValueType),
    ).getOrElse(fail("model not generated"))

    val data = SampledFunction(Seq.empty)

    new MemoizedDataset(metadata, model, data)
  }

  test("encode the ID metadata of the dataset being encoded") {
    new MetadataEncoder().encode(dataset).compile.lastOrError.map { md =>
      val cursor = md.hcursor
      assertEquals(cursor.get[String]("id").getOrElse(fail("Missing dataset ID")), "dataset")
      assertEquals(cursor.get[String]("model").getOrElse(fail("Missing model")), "function: a -> b")

      val variables = cursor.downField("variable")
      assertEquals(variables.downN(0).get[String]("id").getOrElse(fail("Missing variable ID")), "a")
      assertEquals(variables.downN(0).get[String]("type").getOrElse(fail("Missing variable type")), "int")
      assertEquals(variables.downN(1).get[String]("id").getOrElse(fail("Missing variable ID")), "b")
      assertEquals(variables.downN(1).get[String]("type").getOrElse(fail("Missing variable type")), "int")
    }
  }
}

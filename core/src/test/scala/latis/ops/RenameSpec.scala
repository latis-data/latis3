package latis.ops

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier.IdentifierStringContext

class RenameSpec extends AnyFlatSpec {

  "The Rename Operation" should "reconstruct the model with a single variable's name changed" in {
    val model = Function(
      Tuple(
        Scalar(Metadata(id"a") + ("type" -> "int")),
        Scalar(Metadata(id"b") + ("type" -> "int"))
      ),
      Function(
        Scalar(Metadata(id"c") + ("type" -> "int")),
        Tuple(
          Scalar(Metadata(id"d") + ("type" -> "int")),
          Scalar(Metadata(id"e") + ("type" -> "int"))
        )
      )
    )
    val r = Rename(id"e", id"f").applyToModel(model).fold(e => fail(e.message), identity)
    r.toString should be ("(a, b) -> c -> (d, f)")
  }
}

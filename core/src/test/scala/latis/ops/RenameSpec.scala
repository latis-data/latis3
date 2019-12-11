package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.metadata.Metadata
import latis.model._

class RenameSpec extends FlatSpec {

  "The Rename Operation" should "reconstruct the model with a single variable's name changed" in {
    val model = Function(
      Tuple(
        Scalar(Metadata("a") + ("type" -> "int")),
        Scalar(Metadata("b") + ("type" -> "int"))
      ),
      Function(
        Scalar(Metadata("c") + ("type" -> "int")),
        Tuple(
          Scalar(Metadata("d") + ("type" -> "int")),
          Scalar(Metadata("e") + ("type" -> "int"))
        )
      )
    )
    val r = Rename("e", "f").applyToModel(model)
    r.toString should be ("(a, b) -> c -> (d, f)")
  }
}

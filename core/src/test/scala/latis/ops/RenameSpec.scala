package latis.ops

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.dsl.ModelParser
import latis.util.Identifier.IdentifierStringContext

class RenameSpec extends AnyFlatSpec {

  "The Rename Operation" should "reconstruct the model with a single variable's name changed" in {
    val model = ModelParser.unsafeParse("(a, b) -> c -> (d, e)")
    val r = Rename(id"e", id"f").applyToModel(model).fold(e => fail(e.message), identity)
    r.toString should be ("(a, b) -> c -> (d, f)")
  }
}

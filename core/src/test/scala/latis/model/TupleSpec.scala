package latis.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.metadata.Metadata
import latis.util.Identifier.IdentifierStringContext

class TupleSpec extends AnyFlatSpec {
  "A nested tuple" should "flatten into a non-nested tuple" in {
    val nestedTuple = Tuple(
      Tuple(
        Scalar(Metadata(id"a") + ("type" -> "int")),
        Scalar(Metadata(id"b") + ("type" -> "int"))
      ),
      Scalar(Metadata(id"c") + ("type" -> "int"))
    )
    val n = nestedTuple.flatten match {
      case Tuple(es @ _*) => es.length
    }
    n should be(3)
  }
}

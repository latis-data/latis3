package latis.tests

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.metadata.Metadata
import latis.model.Scalar
import latis.model.Tuple
import latis.util.Identifier.IdentifierStringContext

class TupleSpec extends FlatSpec {

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
    n should be (3)
  }
}

package latis.model

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.metadata.Metadata

class TupleSpec extends FlatSpec {

  "A nested tuple" should "flatten into a non-nested tuple" in {
    val nestedTuple = Tuple(
      Tuple(
        Scalar(Metadata("a") + ("type" -> "int")),
        Scalar(Metadata("b") + ("type" -> "int"))
      ),
      Scalar(Metadata("c") + ("type" -> "int"))
    )
    val n = nestedTuple.flatten match {
      case Tuple(es @ _*) => es.length
    }
    n should be (3)
  }
}

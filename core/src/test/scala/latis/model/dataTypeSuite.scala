package latis.model

import latis.metadata.Metadata
import org.scalatest.FunSuite

class TupleFlattenSuite extends FunSuite {
  test("Flattening a nested tuple should prepend tuple IDs to scalar IDs") {
    val nestedTuple = Tuple(Metadata("tup1"),
      Scalar(Metadata("a") + ("type" -> "int")),
      Tuple(Metadata("tup2"),
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int")),
        Tuple(Metadata("tup3"),
          Scalar(Metadata("d") + ("type" -> "int"))
        )
      )
    )

    val flattened = nestedTuple.flatten

    val expectedTuple = Tuple(Metadata("tup1"),
      Scalar(Metadata("tup1.a") + ("type" -> "int")),
      Scalar(Metadata("tup1.tup2.b") + ("type" -> "int")),
      Scalar(Metadata("tup1.tup2.c") + ("type" -> "int")),
      Scalar(Metadata("tup1.tup2.tup3.d") + ("type" -> "int"))
    )

    println(flattened.toString)
    println(expectedTuple.toString)
    println(s"${flattened.id}, ${expectedTuple.id}")

    assert(flattened.toString == expectedTuple.toString)
    assert(flattened.id == expectedTuple.id)
  }

  test("Flattening doubly nested tuple where outermost tuple lacks an ID") {
    val nestedTuple = Tuple(
      Scalar(Metadata("a") + ("type" -> "int")),
      Tuple(Metadata("tup2"),
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int")),
        Tuple(Metadata("tup3"),
          Scalar(Metadata("d") + ("type" -> "int"))
        )
      )
    )

    val flattened = nestedTuple.flatten

    val expectedTuple = Tuple(
      Scalar(Metadata(".a") + ("type" -> "int")),
      Scalar(Metadata(".tup2.b") + ("type" -> "int")),
      Scalar(Metadata(".tup2.c") + ("type" -> "int")),
      Scalar(Metadata(".tup2.tup3.d") + ("type" -> "int"))
    )
    
    assert(flattened.toString == expectedTuple.toString)
    assert(flattened.id == expectedTuple.id)
  }

  test("Flattening doubly nested tuple where middle tuple lacks an ID") {
    val nestedTuple = Tuple(Metadata("tup1"),
      Scalar(Metadata("a") + ("type" -> "int")),
      Tuple(
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int")),
        Tuple(Metadata("tup3"),
          Scalar(Metadata("d") + ("type" -> "int"))
        )
      )
    )

    val flattened = nestedTuple.flatten

    val expectedTuple = Tuple(Metadata("tup1"),
      Scalar(Metadata("tup1.a") + ("type" -> "int")),
      Scalar(Metadata("tup1..b") + ("type" -> "int")),
      Scalar(Metadata("tup1..c") + ("type" -> "int")),
      Scalar(Metadata("tup1..tup3.d") + ("type" -> "int"))
    )
    
    assert(flattened.toString == expectedTuple.toString)
    assert(flattened.id == expectedTuple.id)
  }

  test("Flattening doubly nested tuple where innermost tuple lacks an ID") {
    val nestedTuple = Tuple(Metadata("tup1"),
      Scalar(Metadata("a") + ("type" -> "int")),
      Tuple(Metadata("tup2"),
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int")),
        Tuple(
          Scalar(Metadata("d") + ("type" -> "int"))
        )
      )
    )

    val flattened = nestedTuple.flatten

    val expectedTuple = Tuple(Metadata("tup1"),
      Scalar(Metadata("tup1.a") + ("type" -> "int")),
      Scalar(Metadata("tup1.tup2.b") + ("type" -> "int")),
      Scalar(Metadata("tup1.tup2.c") + ("type" -> "int")),
      Scalar(Metadata("tup1.tup2..d") + ("type" -> "int"))
    )
    
    assert(flattened.toString == expectedTuple.toString)
    assert(flattened.id == expectedTuple.id)
  }

  test("Flattening doubly nested tuple where outermost and middle tuples lack IDs") {
    val nestedTuple = Tuple(
      Scalar(Metadata("a") + ("type" -> "int")),
      Tuple(
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int")),
        Tuple(Metadata("tup3"),
          Scalar(Metadata("d") + ("type" -> "int"))
        )
      )
    )

    val flattened = nestedTuple.flatten

    val expectedTuple = Tuple(
      Scalar(Metadata(".a") + ("type" -> "int")),
      Scalar(Metadata("..b") + ("type" -> "int")),
      Scalar(Metadata("..c") + ("type" -> "int")),
      Scalar(Metadata("..tup3.d") + ("type" -> "int"))
    )

    assert(flattened.toString == expectedTuple.toString)
    assert(flattened.id == expectedTuple.id)
  }

  test("Flattening doubly nested tuple where middle and innermost tuples lack IDs") {
    val nestedTuple = Tuple(Metadata("tup1"),
      Scalar(Metadata("a") + ("type" -> "int")),
      Tuple(
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int")),
        Tuple(
          Scalar(Metadata("d") + ("type" -> "int"))
        )
      )
    )

    val flattened = nestedTuple.flatten

    val expectedTuple = Tuple(Metadata("tup1"),
      Scalar(Metadata("tup1.a") + ("type" -> "int")),
      Scalar(Metadata("tup1..b") + ("type" -> "int")),
      Scalar(Metadata("tup1..c") + ("type" -> "int")),
      Scalar(Metadata("tup1...d") + ("type" -> "int"))
    )

    assert(flattened.toString == expectedTuple.toString)
    assert(flattened.id == expectedTuple.id)
  }
  
  test("Flattening function with two nested tuples") {
    val nestedTuple1 = Tuple(Metadata("tup1"),
      Scalar(Metadata("a") + ("type" -> "int")),
      Tuple(Metadata("tup2"),
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int")),
        Tuple(Metadata("tup3"),
          Scalar(Metadata("d") + ("type" -> "int"))
        )
      )
    )

    val nestedTuple2 = Tuple(
      Scalar(Metadata("a") + ("type" -> "int")),
      Tuple(
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int")),
        Tuple(Metadata("tup3"),
          Scalar(Metadata("d") + ("type" -> "int"))
        )
      )
    )

    val expectedTuple1 = Tuple(Metadata("tup1"),
      Scalar(Metadata("tup1.a") + ("type" -> "int")),
      Scalar(Metadata("tup1.tup2.b") + ("type" -> "int")),
      Scalar(Metadata("tup1.tup2.c") + ("type" -> "int")),
      Scalar(Metadata("tup1.tup2.tup3.d") + ("type" -> "int"))
    )

    val expectedTuple2 = Tuple(
      Scalar(Metadata(".a") + ("type" -> "int")),
      Scalar(Metadata("..b") + ("type" -> "int")),
      Scalar(Metadata("..c") + ("type" -> "int")),
      Scalar(Metadata("..tup3.d") + ("type" -> "int"))
    )
    
    val func = Function(nestedTuple1, nestedTuple2)
    
    val flattened = func.flatten
    
    flattened match {
      case Function(d, r) =>
        assert(d.toString == expectedTuple1.toString)
        assert(r.toString == expectedTuple2.toString)
        assert(d.id == expectedTuple1.id)
        assert(r.id == expectedTuple2.id)
    }
    
  }
  
}

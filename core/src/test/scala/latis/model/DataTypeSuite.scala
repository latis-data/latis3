package latis.model

import latis.data.DomainPosition
import latis.data.RangePosition
import latis.metadata.Metadata
import org.scalatest.FunSuite

class GetPathSuite extends FunSuite {
  //TODO: test searching for a Scalar with a namespace (e.g. "myTup.a")
  
  test("getPath to Tuple in domain") {
    val func = {
      val d = Tuple(Metadata("tup"),
        Scalar(Metadata("a") + ("type" -> "int")),
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int"))
      )
      val r = Scalar(Metadata("d") + ("type" -> "int"))
      Function(d, r)
    }

    assert(func.getPath("tup") == Some(List(DomainPosition(0))))
  }

  test("getPath to nested Tuple in domain") {
    val func = {
      val d = Tuple(
        Scalar(Metadata("a") + ("type" -> "int")),
        Scalar(Metadata("b") + ("type" -> "int")),
        Tuple(Metadata("tup"),
          Scalar(Metadata("c") + ("type" -> "int")),
          Scalar(Metadata("d") + ("type" -> "int")),
          Scalar(Metadata("e") + ("type" -> "int"))
        )
      )
      val r = Scalar(Metadata("f") + ("type" -> "int"))
      Function(d, r)
    }

    assert(func.getPath("tup") == Some(List(DomainPosition(2))))
  }

  test("getPath to Tuple in range") {
    val func = {
      val d = Scalar(Metadata("a") + ("type" -> "int"))
      val r = Tuple(Metadata("tup"),
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int")),
        Scalar(Metadata("d") + ("type" -> "int"))
      )
      Function(d, r)
    }

    assert(func.getPath("tup") == Some(List(RangePosition(0))))
  }

  test("getPath to nested Tuple in range") {
    val func = {
      val d = Scalar(Metadata("a") + ("type" -> "int"))
      val r = Tuple(
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int")),
        Tuple(Metadata("tup"),
          Scalar(Metadata("d") + ("type" -> "int")),
          Scalar(Metadata("e") + ("type" -> "int")),
          Scalar(Metadata("f") + ("type" -> "int"))
        )
      )
      Function(d, r)
    }

    assert(func.getPath("tup") == Some(List(RangePosition(2))))
  }

  test("getPath to Scalar in nested Tuple") {
    val func = {
      val d = Scalar(Metadata("a") + ("type" -> "int"))
      val r = Tuple(
        Scalar(Metadata("b") + ("type" -> "int")),
        Scalar(Metadata("c") + ("type" -> "int")),
        Tuple(Metadata("tup"),
          Scalar(Metadata("d") + ("type" -> "int")),
          Scalar(Metadata("e") + ("type" -> "int")),
          Scalar(Metadata("f") + ("type" -> "int"))
        )
      )
      Function(d, r)
    }

    assert(func.getPath("e") == Some(List(RangePosition(3))))
  }
  
  test("getPath to nonexistent variable") {
    val func = Function(Scalar(Metadata("a") + ("type" -> "int")), Scalar(Metadata("b") + ("type" -> "int")))
    
    assert(func.getPath("tup") == None)
  }
}

class TupleFlattenSuite extends FunSuite {
  test("Flattening doubly nested tuple where all tuples have IDs") {
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
      Scalar(Metadata("a") + ("type" -> "int")),
      Scalar(Metadata("tup2.b") + ("type" -> "int")),
      Scalar(Metadata("tup2.c") + ("type" -> "int")),
      Scalar(Metadata("tup2.tup3.d") + ("type" -> "int"))
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
      Tuple(Metadata("tup2"),
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
      Scalar(Metadata("a") + ("type" -> "int")),
      Scalar(Metadata("tup2.b") + ("type" -> "int")),
      Scalar(Metadata("tup2.c") + ("type" -> "int")),
      Scalar(Metadata("tup2.tup3.d") + ("type" -> "int"))
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

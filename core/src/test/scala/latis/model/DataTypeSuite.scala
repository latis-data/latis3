package latis.model

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.data._
import latis.metadata.Metadata
import latis.util.Identifier.IdentifierStringContext

class GetPathSuite extends AnyFunSuite {
  test("getPath to Tuple in domain") {
    val func = {
      val d = Tuple(Metadata(id"tup"),
        Scalar(Metadata(id"a") + ("type" -> "int")),
        Scalar(Metadata(id"b") + ("type" -> "int")),
        Scalar(Metadata(id"c") + ("type" -> "int"))
      )
      val r = Scalar(Metadata(id"d") + ("type" -> "int"))
      Function(d, r)
    }

    assert(func.getPath(id"tup") == Some(List(DomainPosition(0))))
  }

  test("getPath to nested Tuple in domain") {
    val func = {
      val d = Tuple(
        Scalar(Metadata(id"a") + ("type" -> "int")),
        Scalar(Metadata(id"b") + ("type" -> "int")),
        Tuple(Metadata(id"tup"),
          Scalar(Metadata(id"c") + ("type" -> "int")),
          Scalar(Metadata(id"d") + ("type" -> "int")),
          Scalar(Metadata(id"e") + ("type" -> "int"))
        )
      )
      val r = Scalar(Metadata(id"f") + ("type" -> "int"))
      Function(d, r)
    }

    assert(func.getPath(id"tup") == Some(List(DomainPosition(2))))
  }

  test("getPath to Tuple in range") {
    val func = {
      val d = Scalar(Metadata(id"a") + ("type" -> "int"))
      val r = Tuple(Metadata(id"tup"),
        Scalar(Metadata(id"b") + ("type" -> "int")),
        Scalar(Metadata(id"c") + ("type" -> "int")),
        Scalar(Metadata(id"d") + ("type" -> "int"))
      )
      Function(d, r)
    }

    assert(func.getPath(id"tup") == Some(List(RangePosition(0))))
  }

  test("getPath to nested Tuple in range") {
    val func = {
      val d = Scalar(Metadata(id"a") + ("type" -> "int"))
      val r = Tuple(
        Scalar(Metadata(id"b") + ("type" -> "int")),
        Scalar(Metadata(id"c") + ("type" -> "int")),
        Tuple(Metadata(id"tup"),
          Scalar(Metadata(id"d") + ("type" -> "int")),
          Scalar(Metadata(id"e") + ("type" -> "int")),
          Scalar(Metadata(id"f") + ("type" -> "int"))
        )
      )
      Function(d, r)
    }

    assert(func.getPath(id"tup") == Some(List(RangePosition(2))))
  }

  test("getPath to Scalar in nested Tuple in range") {
    val func = {
      val d = Scalar(Metadata(id"a") + ("type" -> "int"))
      val r = Tuple(
        Scalar(Metadata(id"b") + ("type" -> "int")),
        Scalar(Metadata(id"c") + ("type" -> "int")),
        Tuple(Metadata(id"tup"),
          Scalar(Metadata(id"d") + ("type" -> "int")),
          Scalar(Metadata(id"e") + ("type" -> "int")),
          Scalar(Metadata(id"f") + ("type" -> "int"))
        )
      )
      Function(d, r)
    }

    assert(func.getPath(id"e") == Some(List(RangePosition(3))))
  }

  test("getPath to Scalar in nested Tuple, searching fully qualified ID") {
    val func = {
      val d = Scalar(Metadata(id"a") + ("type" -> "int"))
      val r = Tuple(
        Scalar(Metadata(id"b") + ("type" -> "int")),
        Scalar(Metadata(id"c") + ("type" -> "int")),
        Tuple(Metadata(id"tup"),
          Scalar(Metadata(id"d") + ("type" -> "int")),
          Scalar(Metadata(id"e") + ("type" -> "int")),
          Scalar(Metadata(id"f") + ("type" -> "int"))
        )
      )
      Function(d, r)
    }

    assert(func.getPath(id"tup.e") == Some(List(RangePosition(3))))
  }
  
  test("getPath to nonexistent variable") {
    val func = Function(Scalar(Metadata(id"a") + ("type" -> "int")), Scalar(Metadata(id"b") + ("type" -> "int")))
    
    assert(func.getPath(id"tup") == None)
  }

  test("getPath to lone Scalar") {
    inside(Scalar(Metadata(id"a") + ("type" -> "int")).getPath(id"a")) {
      case Some(RangePosition(p) :: Nil) => assert(p == 0)
    }
  }

  ignore("getPath to Scalar in lone Tuple") {
    inside(Tuple(
      Scalar(Metadata(id"a") + ("type" -> "int")),
      Scalar(Metadata(id"b") + ("type" -> "int"))
    ).getPath(id"b")) {
      case Some(RangePosition(p) :: Nil) => assert(p == 1)
    }
  }

  test("no path to Index variable") {
    val p = Function(
      Index(id"i"),
      Scalar(Metadata(id"a") + ("type" -> "int"))
    ).getPath(id"i")
    assert(p.isEmpty)
  }

  test("getPath ignoring Index variable") {
    // (i, y) -> a
    inside(Function(
      Tuple(Index(id"i"), Scalar(Metadata(id"y") + ("type" -> "int"))),
      Scalar(Metadata(id"a") + ("type" -> "int"))
    ).getPath(id"y")) {
      case Some(DomainPosition(p) :: Nil) =>
        assert(p == 0)
    }
  }
}

class TupleFlattenSuite extends AnyFunSuite {
  test("Flattening doubly nested tuple where all tuples have IDs") {
    val nestedTuple = Tuple(Metadata(id"tup1"),
      Scalar(Metadata(id"a") + ("type" -> "int")),
      Tuple(Metadata(id"tup2"),
        Scalar(Metadata(id"b") + ("type" -> "int")),
        Scalar(Metadata(id"c") + ("type" -> "int")),
        Tuple(Metadata(id"tup3"),
          Scalar(Metadata(id"d") + ("type" -> "int"))
        )
      )
    )

    val flattened = nestedTuple.flatten

    val expectedTuple = Tuple(Metadata(id"tup1"),
      Scalar(Metadata(id"tup1.a") + ("type" -> "int")),
      Scalar(Metadata(id"tup1.tup2.b") + ("type" -> "int")),
      Scalar(Metadata(id"tup1.tup2.c") + ("type" -> "int")),
      Scalar(Metadata(id"tup1.tup2.tup3.d") + ("type" -> "int"))
    )

    assert(flattened.toString == expectedTuple.toString)
    assert(flattened.id == expectedTuple.id)
  }

  test("Flattening doubly nested tuple where outermost tuple lacks an ID") {
    val nestedTuple = Tuple(
      Scalar(Metadata(id"a") + ("type" -> "int")),
      Tuple(Metadata(id"tup2"),
        Scalar(Metadata(id"b") + ("type" -> "int")),
        Scalar(Metadata(id"c") + ("type" -> "int")),
        Tuple(Metadata(id"tup3"),
          Scalar(Metadata(id"d") + ("type" -> "int"))
        )
      )
    )

    val flattened = nestedTuple.flatten

    val expectedTuple = Tuple(
      Scalar(Metadata(id"a") + ("type" -> "int")),
      Scalar(Metadata(id"tup2.b") + ("type" -> "int")),
      Scalar(Metadata(id"tup2.c") + ("type" -> "int")),
      Scalar(Metadata(id"tup2.tup3.d") + ("type" -> "int"))
    )

    assert(flattened.toString == expectedTuple.toString)
    assert(flattened.id == expectedTuple.id)
  }

  test("Flattening function with two nested tuples") {
    val nestedTuple1 = Tuple(Metadata(id"tup1"),
      Scalar(Metadata(id"a") + ("type" -> "int")),
      Tuple(Metadata(id"tup2"),
        Scalar(Metadata(id"b") + ("type" -> "int")),
        Scalar(Metadata(id"c") + ("type" -> "int")),
        Tuple(Metadata(id"tup3"),
          Scalar(Metadata(id"d") + ("type" -> "int"))
        )
      )
    )

    val nestedTuple2 = Tuple(
      Scalar(Metadata(id"a") + ("type" -> "int")),
      Tuple(Metadata(id"tup2"),
        Scalar(Metadata(id"b") + ("type" -> "int")),
        Scalar(Metadata(id"c") + ("type" -> "int")),
        Tuple(Metadata(id"tup3"),
          Scalar(Metadata(id"d") + ("type" -> "int"))
        )
      )
    )

    val expectedTuple1 = Tuple(Metadata(id"tup1"),
      Scalar(Metadata(id"tup1.a") + ("type" -> "int")),
      Scalar(Metadata(id"tup1.tup2.b") + ("type" -> "int")),
      Scalar(Metadata(id"tup1.tup2.c") + ("type" -> "int")),
      Scalar(Metadata(id"tup1.tup2.tup3.d") + ("type" -> "int"))
    )

    val expectedTuple2 = Tuple(
      Scalar(Metadata(id"a") + ("type" -> "int")),
      Scalar(Metadata(id"tup2.b") + ("type" -> "int")),
      Scalar(Metadata(id"tup2.c") + ("type" -> "int")),
      Scalar(Metadata(id"tup2.tup3.d") + ("type" -> "int"))
    )

    val func = Function(nestedTuple1, nestedTuple2)

    val flattened = func.flatten

    inside(flattened) {
      case Function(d, r) =>
        assert(d.toString == expectedTuple1.toString)
        assert(r.toString == expectedTuple2.toString)
        assert(d.id == expectedTuple1.id)
        assert(r.id == expectedTuple2.id)
    }
  }
}

class ParseValueSuite extends AnyFunSuite {

  test("Replace invalid value with missing value") {
    val s = Scalar(Metadata(
      "id" -> "a",
      "type" -> "double",
      "fillValue" -> "NaN"
    ))
    inside(s.parseValue("word")) {
      case Right(d: Data.DoubleValue) =>
        assert(d.value.isNaN)
    }
  }
}

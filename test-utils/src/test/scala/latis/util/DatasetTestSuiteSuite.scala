package latis.util

import cats.effect.IO
import fs2.Stream
import org.scalatest.exceptions.TestFailedException
import org.scalatest.funsuite.AnyFunSuite

import latis.catalog.Catalog
import latis.data._
import latis.dataset.Dataset
import latis.dsl._
import latis.model._
import latis.ops._
import latis.util.Identifier.IdentifierStringContext

class DatasetTestSuiteSuite extends AnyFunSuite {

  val testSuite = new DatasetTestSuite {
    val catalog: Catalog = new Catalog {
      def datasets: Stream[IO, Dataset] = Stream.emits{
        List(
          DatasetGenerator("x -> a: string", id"foo"),
          DatasetGenerator("(x, y) -> a", id"bar2D").curry(1),
        )
      }
    }
  }

  test("dataset") {
    testSuite.withDataset(id"foo") { ds =>
      assert(ds.id.get.asString == "foo")
    }
  }

  test("dataset not found") {
    val x = intercept[TestFailedException]{
      testSuite.withDataset(id"bar")(_ => ???)
    }
    assert(x.getMessage == "Dataset not found: bar")
  }

  test("dataset with operation") {
    val ops = List(Take(2))
    testSuite.withDataset(id"foo", ops) { ds =>
      val h = ds.metadata.getProperty("history").getOrElse(fail("No history found"))
      assert(h.contains("Take"))
    }
  }

  test("match model") {
    testSuite.matchModel(id"foo") {
      case Function(x: Scalar, a: Scalar) =>
        assert(x.id.asString == "x")
        assert(a.id.asString == "a")
    }
  }

  test("match model with operation") {
    val ops = List(Rename(id"a", id"b"))
    testSuite.matchModel(id"foo", ops) {
      case Function(x: Scalar, a: Scalar) =>
        assert(x.id.asString == "x")
        assert(a.id.asString == "b")
    }
  }

  test("match first sample") {
    testSuite.matchFirstSample(id"foo") {
      case Sample(DomainData(Integer(x)), RangeData(Text(a))) =>
        assert(x == 0)
        assert(a == "a")
    }
  }

  test("match first sample with operation") {
    val ops = List(Drop(1))
    testSuite.matchFirstSample(id"foo", ops) {
      case Sample(DomainData(Integer(x)), RangeData(Text(a))) =>
        assert(x == 1)
        assert(a == "b")
    }
  }

  test("empty dataset") {
    val ops = List(Drop(10))
    val x = intercept[TestFailedException] {
      testSuite.matchFirstSample(id"foo", ops)(_ => ???)
    }
    assert(x.getMessage == "Empty dataset")
  }

  test("equals first sample") {
    val ops = List(Drop(1))
    testSuite.equalsFirstSample(id"foo", ops)(1, "b")
  }

  test("equals first sample with list") {
    val ops = List(Drop(1))
    val values: List[Any] = List(1, "b")
    testSuite.equalsFirstSample(id"foo", ops)(values: _*)
  }

  test("equals fails for nested function") {
    val values = List(0, 0, 0)
    val x = intercept[TestFailedException] {
      testSuite.equalsFirstSample(id"bar2D")(values)
    }
    assert(x.getMessage() == "Equality test on Function not supported")
  }
}

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

class TestUtilsSuite extends AnyFunSuite {

  val testUtils = new DatasetTestUtils {
    val catalog: Catalog = new Catalog {
      def datasets: Stream[IO, Dataset] = Stream.emits{
        List(
          DatasetGenerator("x -> a", id"foo"),
          DatasetGenerator("(x, y) -> a", id"bar2D").curry(1),
        )
      }
    }
  }

  test("dataset") {
    testUtils.withDataset(id"foo") { ds =>
      assert(ds.id.get.asString == "foo")
    }
  }

  test("dataset not found") {
    val x = intercept[TestFailedException]{
      testUtils.withDataset(id"bar")(_ => ???)
    }
    assert(x.getMessage == "Dataset not found: bar")
  }

  test("dataset with operation") {
    val ops = List(Take(2))
    testUtils.withDataset(id"foo", ops) { ds =>
      val h = ds.metadata.getProperty("history").getOrElse(fail("No history found"))
      assert(h.contains("Take"))
    }
  }

  test("match model") {
    testUtils.matchModel(id"foo") {
      case Function(x: Scalar, a: Scalar) =>
        assert(x.id.get.asString == "x")
        assert(a.id.get.asString == "a")
    }
  }

  test("match model with operation") {
    val ops = List(Rename(id"a", id"b"))
    testUtils.matchModel(id"foo", ops) {
      case Function(x: Scalar, a: Scalar) =>
        assert(x.id.get.asString == "x")
        assert(a.id.get.asString == "b")
    }
  }

  test("match first sample") {
    testUtils.matchFirstSample(id"foo") {
      case Sample(DomainData(Integer(x)), RangeData(Integer(a))) =>
        assert(x == 0)
        assert(a == 0)
    }
  }

  test("match first sample with operation") {
    val ops = List(Drop(1))
    testUtils.matchFirstSample(id"foo", ops) {
      case Sample(DomainData(Integer(x)), RangeData(Integer(a))) =>
        assert(x == 1)
        assert(a == 1)
    }
  }

  test("empty dataset") {
    val ops = List(Drop(10))
    val x = intercept[TestFailedException] {
      testUtils.matchFirstSample(id"foo", ops)(_ => ???)
    }
    assert(x.getMessage == "Empty dataset")
  }

  test("equals first sample") {
    val ops = List(Drop(1))
    val values = List(1, 1)
    testUtils.equalsFirstSample(id"foo", ops)(values)
  }

  test("equals fails for nested function") {
    val values = List(0, 0, 0)
    val x = intercept[TestFailedException] {
      testUtils.equalsFirstSample(id"bar2D")(values)
    }
    assert(x.getMessage() == "Equality test on Function not supported")
  }
}

package latis.dataset

import cats.effect.unsafe.implicits.global
import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.data._
import latis.dsl._
import latis.metadata.Metadata
import latis.ops._
import latis.util.Identifier.IdentifierStringContext

class GranuleAppendDatasetSuite extends AnyFunSuite {

  private lazy val granuleList: Dataset = DatasetGenerator("t: double -> uri: string")
  /*
    t -> uri
    0.0 -> a
    1.0 -> b
    2.0 -> c
   */

  private lazy val model = ModelParser("t: double -> a: string").value

  //TODO: concurrency issue?
  private lazy val counter = scala.collection.mutable.Map[String, Int]()

  /** Converts granuleList sample into 2-sample Dataset. */
  private lazy val granuleToDataset: Sample => Dataset = {
    case Sample(DomainData(Number(t)), RangeData(Text(u))) =>
      // Count which granules are accessed
      counter.get(u) match {
        case Some(n) => counter += u -> (n + 1)
        case None    => counter += u -> 1
      }
      val mf: MemoizedFunction = SeqFunction(List(
        Sample(DomainData(t), RangeData(u + t)),
        Sample(DomainData(t + 0.5), RangeData(u + (t + 0.5)))
      ))
      new MemoizedDataset(
        Metadata("id" -> s"granule_$u"),
        model,
        mf
      )
    case _ => throw new RuntimeException("Invalid Sample")
  }

  private lazy val dataset = GranuleAppendDataset(
    id"myDataset",
    granuleList,
    model,
    granuleToDataset
  )

  test("join all") {
    dataset.samples.map {
      inside(_) {
        case Sample(DomainData(Number(t)), _) => t
      }
    }.compile.toList.map { ts =>
      assert(ts == List(0.0, 0.5, 1.0, 1.5, 2.0, 2.5))
    }.unsafeRunSync()
  }

  test("push down selection") {
    counter.clear()
    val ops = List(
      Selection.makeSelection("t <= 1").value
    )
    dataset.withOperations(ops).samples.map {
      inside(_) {
        case Sample(DomainData(Number(t)), _) => t
      }
    }.compile.toList.map { ts =>
      assert(ts == List(0.0, 0.5, 1.0))
      assert(!counter.contains("c")) //never accesses granule c
    }.unsafeRunSync()
  }

  test("push down selection after filter") {
    counter.clear()
    val ops = List(
      Selection.makeSelection("a < b").value, //not pushed down
      Selection.makeSelection("t <= 1").value
    )
    dataset.withOperations(ops).samples.map {
      inside(_) {
        case Sample(DomainData(Number(t)), _) => t
      }
    }.compile.toList.map { ts =>
      assert(ts == List(0.0, 0.5))
      assert(!counter.contains("c")) //never accesses granule c
    }.unsafeRunSync()
  }

  test("push down selection after take") {
    counter.clear()
    val ops = List(
      Take(3),
      Selection.makeSelection("t <= 1").value
    )
    dataset.withOperations(ops).samples.map {
      inside(_) {
        case Sample(DomainData(Number(t)), _) => t
      }
    }.compile.toList.map { ts =>
      assert(ts == List(0.0, 0.5, 1.0))
      assert(!counter.contains("c")) //never accesses granule c
    }.unsafeRunSync()
  }

  test("push down selection after t rename") {
    counter.clear()
    val ops = List(
      Rename(id"t", id"foo"),
      Selection.makeSelection("foo <= 1").value
    )
    dataset.withOperations(ops).samples.map {
      inside(_) {
        case Sample(DomainData(Number(t)), _) => t
      }
    }.compile.toList.map { ts =>
      assert(ts == List(0.0, 0.5, 1.0))
      assert(!counter.contains("c")) //never accesses granule c
    }.unsafeRunSync()
  }

  test("push down selection after other rename") {
    counter.clear()
    val ops = List(
      Rename(id"a", id"bar"),
      Selection.makeSelection("t <= 1").value
    )
    dataset.withOperations(ops).samples.map {
      inside(_) {
        case Sample(DomainData(Number(t)), _) => t
      }
    }.compile.toList.map { ts =>
      assert(ts == List(0.0, 0.5, 1.0))
      assert(!counter.contains("c")) //never accesses granule c
    }.unsafeRunSync()
  }

  //TODO: add another range variable to unproject
  //TODO: test handling of Index
  ignore("push down selection after projection") {}

  test("don't push down selection after stride") {
    counter.clear()
    val ops = List(
      Stride(2),
      Selection.makeSelection("t <= 1").value
    )
    dataset.withOperations(ops).samples.map {
      inside(_) {
        case Sample(DomainData(Number(t)), _) => t
      }
    }.compile.toList.map { ts =>
      assert(ts == List(0.0, 1.0))
      assert(counter.contains("c")) //does accesses granule c
    }.unsafeRunSync()
  }

  //TODO: seems to access 3rd granule though not needed?
  ignore("take short circuit") {
    counter.clear()
    val ops = List(
      Take(3)
    )
    dataset.withOperations(ops).samples.map {
      inside(_) {
        case Sample(DomainData(Number(t)), _) => t
      }
    }.compile.toList.map { ts =>
      assert(ts == List(0.0, 0.5, 1.0))
      assert(!counter.contains("c")) //never accesses granule c
    }.unsafeRunSync()
  }

  ignore("single granule") {}

  ignore("empty dataset") {}

  ignore("error in granuleToDataset") {}

  ignore("push down selection with bin semantics") {}
}

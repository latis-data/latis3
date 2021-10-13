package latis.dataset

import cats.effect.unsafe.implicits.global
import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.data._
import latis.dsl._
import latis.metadata.Metadata
import latis.model._
import latis.ops._
import latis.util.Identifier.IdentifierStringContext

class GranuleAppendDatasetSuite extends AnyFunSuite {

  /*
    Granule list dataset:
    t -> uri
    0.0 -> a
    1.0 -> b
    2.0 -> c
   */
  private lazy val granuleList: Dataset = {
    val model = Function.from(
      Scalar.fromMetadata(Metadata("id" -> "t", "type" -> "double", "binWidth" -> "1.0")).value,
      Scalar.fromMetadata(Metadata("id" -> "uri", "type" -> "string")).value
    ).value

    val samples = List(
      Sample(DomainData(0.0), RangeData("a")),
      Sample(DomainData(1.0), RangeData("b")),
      Sample(DomainData(2.0), RangeData("c"))
    )

    new MemoizedDataset(Metadata(id"test"), model, SeqFunction(samples))
  }

  private lazy val model = ModelParser("t: double -> (a: string, b: int)").value

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
        Sample(DomainData(t), RangeData(u + t, t.toInt)),
        Sample(DomainData(t + 0.5), RangeData(u + (t + 0.5), t.toInt))
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

  test("push down selection with partial granule") {
    counter.clear()
    val ops = List(
      Selection.makeSelection("t > 1.1").value //">" changed to ">=" to get partial granule
    )
    dataset.withOperations(ops).samples.map {
      inside(_) {
        case Sample(DomainData(Number(t)), _) => t
      }
    }.compile.toList.map { ts =>
      assert(ts == List(1.5, 2.0, 2.5))
      assert(!counter.contains("a")) //never accesses granule a
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

  test("push down selection after projection") {
    counter.clear()
    val ops = List(
      Projection(id"t", id"a"),
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

  //TODO: seems to access 3rd granule though not needed, CompositeDataset issue?
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

  test("single granule") {
    counter.clear()
    val ops = List(
      Selection.makeSelection("t = 1.5").value
    )
    dataset.withOperations(ops).samples.map {
      inside(_) {
        case Sample(DomainData(Number(t)), _) => t
      }
    }.compile.toList.map { ts =>
      assert(ts == List(1.5))
      assert(counter.size == 1)
    }.unsafeRunSync()
  }

  test("empty dataset") {
    counter.clear()
    val ops = List(
      Selection.makeSelection("t > 3").value
    )
    dataset.withOperations(ops).samples.map {
      inside(_) {
        case Sample(DomainData(Number(t)), _) => t
      }
    }.compile.toList.map { ts =>
      assert(ts.isEmpty)
      assert(counter.isEmpty)
    }.unsafeRunSync()
  }

  test("error in granuleToDataset") {
    val f: Sample => Dataset = (_: Sample) => throw new RuntimeException("Boom")
    val ds = GranuleAppendDataset(id"myDataset", granuleList, model, f)
    ds.samples.compile.toList.attempt.map {
      inside(_) {
        case Left(re: RuntimeException) => assert(re.getMessage == "Boom")
      }
    }.unsafeRunSync()
  }
}
package latis.dataset

import java.net.URI

import cats.syntax.all._
import munit.CatsEffectSuite

import latis.data._
import latis.dsl._
import latis.input.Adapter
import latis.metadata.Metadata
import latis.model._
import latis.ops._
import latis.util.Identifier._

class GranuleAppendDatasetSuite extends CatsEffectSuite {

  /*
    Granule list dataset:
    t -> uri
    0.0 -> a
    1.0 -> b
    2.0 -> c
   */
  private lazy val granuleList: Dataset = {
    val model = (
      Scalar.fromMetadata(Metadata("id" -> "t", "type" -> "double", "binWidth" -> "1.0")),
      Scalar.fromMetadata(Metadata("id" -> "uri", "type" -> "string"))
    ).flatMapN(Function.from).fold(fail("failed to create model", _), identity)

    val samples = List(
      Sample(DomainData(0.0), RangeData("a")),
      Sample(DomainData(1.0), RangeData("b")),
      Sample(DomainData(2.0), RangeData("c"))
    )

    new MemoizedDataset(Metadata(id"test"), model, SeqFunction(samples))
  }

  private lazy val model =
    ModelParser("t: double -> (a: string, b: int)")
      .fold(fail("failed to create model", _), identity)

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
    Metadata(id"myDataset"),
    granuleList,
    model,
    granuleToDataset
  )

  test("join all") {
    dataset.samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.assertEquals(
      List(0.0, 0.5, 1.0, 1.5, 2.0, 2.5)
    )
  }

  test("push down selection") {
    counter.clear()

    val ops = List(
      Selection.makeSelection("t <= 1")
    ).sequence.fold(fail("failed to create ops", _), identity)

    dataset.withOperations(ops).samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.map { ts =>
      assertEquals(ts, List(0.0, 0.5, 1.0))
      assert(!counter.contains("c")) //never accesses granule c
    }
  }

  test("push down selection with partial granule") {
    counter.clear()

    val ops = List(
      Selection.makeSelection("t > 1.1") //">" changed to ">=" to get partial granule
    ).sequence.fold(fail("failed to create ops", _), identity)

    dataset.withOperations(ops).samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.map { ts =>
      assertEquals(ts, List(1.5, 2.0, 2.5))
      assert(!counter.contains("a")) //never accesses granule a
    }
  }

  test("push down selection after filter") {
    counter.clear()

    val ops = List(
      Selection.makeSelection("a < b"), //not pushed down
      Selection.makeSelection("t <= 1")
    ).sequence.fold(fail("failed to create ops", _), identity)

    dataset.withOperations(ops).samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.map { ts =>
      assertEquals(ts, List(0.0, 0.5))
      assert(!counter.contains("c")) //never accesses granule c
    }
  }

  test("push down selection after take") {
    counter.clear()

    val ops = List(
      Take(3).asRight,
      Selection.makeSelection("t <= 1")
    ).sequence.fold(fail("failed to create ops", _), identity)

    dataset.withOperations(ops).samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.map { ts =>
      assertEquals(ts, List(0.0, 0.5, 1.0))
      assert(!counter.contains("c")) //never accesses granule c
    }
  }

  test("push down selection after t rename") {
    counter.clear()

    val ops = List(
      Rename(id"t", id"foo").asRight,
      Selection.makeSelection("foo <= 1")
    ).sequence.fold(fail("failed to create ops", _), identity)

    dataset.withOperations(ops).samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.map { ts =>
      assertEquals(ts, List(0.0, 0.5, 1.0))
      assert(!counter.contains("c")) //never accesses granule c
    }
  }

  test("push down selection after other rename") {
    counter.clear()

    val ops = List(
      Rename(id"a", id"bar").asRight,
      Selection.makeSelection("t <= 1")
    ).sequence.fold(fail("failed to create ops", _), identity)

    dataset.withOperations(ops).samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.map { ts =>
      assertEquals(ts, List(0.0, 0.5, 1.0))
      assert(!counter.contains("c")) //never accesses granule c
    }
  }

  test("push down selection after projection") {
    counter.clear()

    val ops = List(
      Projection(id"t", id"a").asRight,
      Selection.makeSelection("t <= 1")
    ).sequence.fold(fail("failed to create ops", _), identity)

    dataset.withOperations(ops).samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.map { ts =>
      assertEquals(ts, List(0.0, 0.5, 1.0))
      assert(!counter.contains("c")) //never accesses granule c
    }
  }

  test("don't push down selection after stride") {
    counter.clear()

    val ops = List(
      Stride(2).asRight,
      Selection.makeSelection("t <= 1")
    ).sequence.fold(fail("failed to create ops", _), identity)

    dataset.withOperations(ops).samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.map { ts =>
      assertEquals(ts, List(0.0, 1.0))
      assert(counter.contains("c")) //does accesses granule c
    }
  }

  //TODO: seems to access 3rd granule though not needed, CompositeDataset issue?
  test("take short circuit".ignore) {
    counter.clear()

    val ops = List(
      Take(3)
    )

    dataset.withOperations(ops).samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.map { ts =>
      assertEquals(ts, List(0.0, 0.5, 1.0))
      assert(!counter.contains("c")) //never accesses granule c
    }
  }

  test("single granule") {
    counter.clear()

    val ops = List(
      Selection.makeSelection("t = 1.5")
    ).sequence.fold(fail("failed to create ops", _), identity)

    dataset.withOperations(ops).samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.map { ts =>
      assertEquals(ts, List(1.5))
      assertEquals(counter.size, 1)
    }
  }

  test("empty dataset") {
    counter.clear()

    val ops = List(
      Selection.makeSelection("t > 3")
    ).sequence.fold(fail("failed to create ops", _), identity)

    dataset.withOperations(ops).samples.map {
      case Sample(DomainData(Number(t)), _) => t
      case _ => fail("got unexpected sample")
    }.compile.toList.map { ts =>
      assert(ts.isEmpty)
      assert(counter.isEmpty)
    }
  }

  test("error in granuleToDataset skips granule") {
    val f: Sample => Dataset = {
      var cnt = -1
      (sample: Sample) =>
        cnt += 1
        if (cnt == 1) throw new RuntimeException("Boom")
        else granuleToDataset(sample)
    }

    val ds = GranuleAppendDataset(Metadata(id"myDataset"), granuleList, model, f)

    ds.samples.compile.toList.map { ss =>
      assertEquals(ss.length, 4)
    }
  }

  test("with adapter") {
    // Make an adapter that provides a 2-sample function based on the uri.
    // This should be the same as the dataset above.
    val adapter = new Adapter {
      var t = -1.0
      def getData(uri: URI, ops: Seq[Operation]): Data = {
        t += 1
        val u = uri.toString
        SeqFunction(List(
          Sample(DomainData(t), RangeData(u + t, t.toInt)),
          Sample(DomainData(t + 0.5), RangeData(u + (t + 0.5), t.toInt))
        ))
      }
    }

    val ds = GranuleAppendDataset
      .withAdapter(Metadata(id"myDataset"), granuleList, model, adapter)
      .fold(fail("failed to create dataset", _), identity)

    ds.samples.compile.toList.map { ss =>
      assertEquals(ss.length, 6)
    }
  }
}

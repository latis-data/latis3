package latis.ops

import cats.effect.IO
import fs2.Stream
import munit.CatsEffectSuite
import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.effect.PropF

import latis.data.*
import latis.data.Data.IntValue
import latis.dataset.TappedDataset
import latis.metadata.Metadata
import latis.model.*
import latis.ops.HorizontalJoinType.*
import latis.util.Identifier.*

/**
 * Generates random Datasets to check that HorizontalJoins
 * behave as expected.
 */
class HorizontalJoinCheck extends CatsEffectSuite with ScalaCheckEffectSuite {

  private val model = {
    val x = Scalar(id"x", IntValueType)
    val a = Scalar.fromMetadata(Metadata(
      "id" -> "x",
      "type" -> "int",
      "fillValue" -> "-9"
    )).fold(throw _, identity)
    Function.from(x, a).fold(throw _, identity)
  }

  private val datasetGen = for {
    size <- Gen.choose(0, 10) //number of samples
    chunk <- Gen.choose(1, 10) //chunk size
    start <- Gen.choose(0, 10) //initial domain value
    steps <- Gen.listOfN(size, Gen.choose(1, 10)) //deltas
  } yield {
    val vs = steps.scanLeft(start)(_ + _)
    val samples = vs.map { v =>
      Sample(DomainData(v), RangeData(v))
    }
    val stream = Stream.emits[IO, Sample](samples)
      .chunkN(chunk).unchunks
    TappedDataset(
      Metadata(id"test"),
      model,
      StreamFunction(stream)
    )
  }

  test("check full outer join length") {
    PropF.forAllF(datasetGen, datasetGen) { (ds1, ds2) =>
      for {
        ds <- IO.fromEither(HorizontalJoin(Full).combine(ds1, ds2))
        n <- ds.samples.compile.toList.map(_.size)
        ex <- (ds1.samples ++ ds2.samples).map { sample =>
          sample.domain.head match {
            case x: IntValue => x.value
          }
        }.compile.toList.map(_.distinct.size)
      } yield assert(n == ex)
    }
  }

  test("check full outer join order") {
    PropF.forAllF(datasetGen, datasetGen) { (ds1, ds2) =>
      for {
        ds <- IO.fromEither(HorizontalJoin(Full).combine(ds1, ds2))
        vs <- ds.samples.compile.toList.map { ss =>
          ss.map {
            case Sample(DomainData(x: IntValue), _) => x.value
          }
        }
      } yield {
        if (vs.size >= 2) assert(vs.sliding(2).forall(p => p(1) > p(0)))
        else assert(true)
      }
    }
  }

  test("check left outer join length") {
    PropF.forAllF(datasetGen, datasetGen) { (ds1, ds2) =>
      for {
        ds <- IO.fromEither(HorizontalJoin(Left).combine(ds1, ds2))
        n  <- ds.samples.compile.toList.map(_.size)
        ex <- ds1.samples.compile.toList.map(_.size)
      } yield assert(n == ex)
    }
  }

  test("check left outer join order") {
    PropF.forAllF(datasetGen, datasetGen) { (ds1, ds2) =>
      for {
        ds <- IO.fromEither(HorizontalJoin(Left).combine(ds1, ds2))
        vs <- ds.samples.compile.toList.map { ss =>
          ss.map {
            case Sample(DomainData(x: IntValue), _) => x.value
          }
        }
      } yield {
        if (vs.size >= 2) assert(vs.sliding(2).forall(p => p(1) > p(0)))
        else assert(true)
      }
    }
  }

  test("check right outer join length") {
    PropF.forAllF(datasetGen, datasetGen) { (ds1, ds2) =>
      for {
        ds <- IO.fromEither(HorizontalJoin(Right).combine(ds1, ds2))
        n  <- ds.samples.compile.toList.map(_.size)
        ex <- ds2.samples.compile.toList.map(_.size)
      } yield assert(n == ex)
    }
  }

  test("check right outer join order") {
    PropF.forAllF(datasetGen, datasetGen) { (ds1, ds2) =>
      for {
        ds <- IO.fromEither(HorizontalJoin(Right).combine(ds1, ds2))
        vs <- ds.samples.compile.toList.map { ss =>
          ss.map {
            case Sample(DomainData(x: IntValue), _) => x.value
          }
        }
      } yield {
        if (vs.size >= 2) assert(vs.sliding(2).forall(p => p(1) > p(0)))
        else assert(true)
      }
    }
  }

  test("check inner join length") {
    PropF.forAllF(datasetGen, datasetGen) { (ds1, ds2) =>
      for {
        ds <- IO.fromEither(HorizontalJoin(Inner).combine(ds1, ds2))
        n  <- ds.samples.compile.toList.map(_.size)
        ex <- for {
          vs1 <- ds1.samples.compile.toList
          vs2 <- ds2.samples.compile.toList
        } yield {
          vs1.toSet.intersect(vs2.toSet).size
        }
      } yield assert(n == ex)
    }
  }

  test("check inner outer join order") {
    PropF.forAllF(datasetGen, datasetGen) { (ds1, ds2) =>
      for {
        ds <- IO.fromEither(HorizontalJoin(Inner).combine(ds1, ds2))
        vs <- ds.samples.compile.toList.map { ss =>
          ss.map {
            case Sample(DomainData(x: IntValue), _) => x.value
          }
        }
      } yield {
        if (vs.size >= 2) assert(vs.sliding(2).forall(p => p(1) > p(0)))
        else assert(true)
      }
    }
  }

}

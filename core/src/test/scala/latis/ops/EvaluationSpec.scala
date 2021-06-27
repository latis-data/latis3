package latis.ops

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside

import latis.data._
import latis.dsl._

class EvaluationSpec extends AnyFlatSpec {

  "Evaluation" should "evaluate a 1D dataset" in {
    DatasetGenerator.generate1DDataset(
      Vector(0, 1, 2),
      Vector(10, 20, 30)
    ).withOperation(Evaluation("1")).samples.head.map {
      case Sample(_, RangeData(Number(d))) =>
        d should be (20)
      case _ => ???
    }.compile.drain.unsafeRunSync()
  }

  "Evaluation" should "evaluate a nested dataset" in {
    val ds = DatasetGenerator.generate2DDataset(
      Vector(0, 1, 2),
      Vector(100, 200, 300),
      Vector(
        Vector(10, 20, 30),
        Vector(12, 22, 32),
        Vector(14, 24, 34)
      )
    ).curry(1)
     .eval("1")
    inside(ds.unsafeForce().data.sampleSeq.head) {
      case Sample(DomainData(Number(x)), RangeData(Number(a))) =>
        x should be (100)
        a should be (12)
    }
  }

  //TODO verify failure modes
}

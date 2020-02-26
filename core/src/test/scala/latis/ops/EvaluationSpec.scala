package latis.ops

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data._
import latis.util.DatasetGenerator

class EvaluationSpec extends FlatSpec {

  "Evaluation" should "evaluate a 1D dataset" in {
    DatasetGenerator.generate1DDataset(
      Vector(0, 1, 2),
      Vector(10, 20, 30)
    ).withOperation(Evaluation(1)).unsafeForce().data match {
      case ConstantFunction(Number(d)) =>
        d should be (20)
    }
  }

  "Evaluation" should "evaluate a 2D dataset" in {
    val d = TupleData(1, 4)
    DatasetGenerator.generate2DDataset(
      Vector(0, 1, 2),
      Vector(3, 4),
      Vector(
        Vector(10, 20),
        Vector(12, 22),
        Vector(14, 24)
      )
    ).withOperation(Evaluation(d)).unsafeForce().data match {
      case ConstantFunction(Number(d)) =>
        d should be (22)
    }
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
     .eval(1)
    ds.unsafeForce().data.sampleSeq.head match {
      case Sample(_, RangeData(mf: MemoizedFunction)) =>
        mf.sampleSeq.head match {
          case Sample(DomainData(Number(x)), RangeData(Number(a))) =>
            x should be (100)
            a should be (12)
        }
    }
  }

  //TODO verify failure modes
}

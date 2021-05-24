package latis.ops

import cats.syntax.all._
import org.scalatest.funsuite.AnyFunSuite

import latis.data.RangeData
import latis.data._
import latis.dsl._
import latis.model.DataType
import latis.util.LatisException

class MapOperationSuite extends AnyFunSuite {

  test("Skip sample that throws") {
    // Apply operation that throws if a = 1 (2nd sample of 3)
    val ds = DatasetGenerator("a -> b").withOperation {
      new MapOperation {
        def mapFunction(model: DataType): Sample => Sample =
          (sample: Sample) => sample match {
            case s@Sample(DomainData(Integer(a)), _) =>
              if (a != 1) s else throw new LatisException("skip")
          }
        def applyToModel(model: DataType) = model.asRight
      }
    }
    assert(2 == ds.samples.compile.toList.unsafeRunSync().length)
  }
}

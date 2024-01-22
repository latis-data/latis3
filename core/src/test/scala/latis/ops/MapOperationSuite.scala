package latis.ops

import cats.syntax.all.*
import munit.CatsEffectSuite

import latis.data.*
import latis.dsl.*
import latis.model.DataType
import latis.util.LatisException

class MapOperationSuite extends CatsEffectSuite {

  test("Skip sample that throws") {
    // Apply operation that throws if a = 1 (2nd sample of 3)
    val ds = DatasetGenerator("a -> b").withOperation {
      new MapOperation {
        def mapFunction(model: DataType): Sample => Sample =
          (sample: Sample) => sample match {
            case s@Sample(DomainData(Integer(a)), _) =>
              if (a != 1) s else throw new LatisException("skip")
            case _ => fail("")
          }
        def applyToModel(model: DataType) = model.asRight
      }
    }

    ds.samples.compile.count.assertEquals(2L)
  }
}

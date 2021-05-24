package latis.dataset

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside

import latis.dsl._
import latis.ops.Head

class MemoizedDatasetSuite extends AnyFunSuite {

  val ds: MemoizedDataset = DatasetGenerator("x -> a")

  test("apply operations when getting data") {
    inside(ds.withOperation(Head())) {
      case mds: MemoizedDataset =>
        assert(mds.data.sampleSeq.length == 1)
    }
  }

}

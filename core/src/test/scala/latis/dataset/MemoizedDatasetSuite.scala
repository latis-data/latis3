package latis.dataset

import munit.FunSuite

import latis.dsl.*
import latis.ops.Head

class MemoizedDatasetSuite extends FunSuite {

  val ds: MemoizedDataset = DatasetGenerator("x -> a")

  test("apply operations when getting data") {
    ds.withOperation(Head()) match {
      case mds: MemoizedDataset =>
        assertEquals(mds.data.sampleSeq.length, 1)
      case _ => fail("memoized dataset not generated")
    }
  }

}

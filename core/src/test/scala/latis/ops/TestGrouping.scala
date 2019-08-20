package latis.ops

import latis.data._
import latis.metadata._
import latis.model._

import org.junit._
import latis.output.TextWriter

class TestGrouping {
  
  val dataset = {
    val metadata = Metadata("test_grouping")
    
    // x -> a
    val model = Function(
      Scalar(Metadata("id" -> "x", "type" -> "double")),
      Scalar(Metadata("id" -> "a", "type" -> "double"))
    )
    
    val domainSet = BinSet1D(0.3, 0.0, 30)
    val range = for {
      i <- (0 until domainSet.length)
    } yield RangeData(i * 10.0)
    val data = SetFunction(domainSet, range)
    
    Dataset(metadata, model, data)
  }
  
  @Test
  def group_by_bin() = {
    val domainSet = BinSet1D(1.0, 1.0, 10)
    
    val ds = GroupByBin(domainSet, NearestNeighborAggregation())(dataset)
    
    TextWriter().write(ds)
  }
    
}
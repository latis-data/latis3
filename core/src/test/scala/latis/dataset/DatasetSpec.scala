package latis.dataset

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data._
import latis.data.Data._
import latis.metadata.Metadata
import latis.model._
import latis.ops.Selection
import latis.util.Identifier.IdentifierStringContext
import latis.util.StreamUtils

class DatasetSpec extends FlatSpec {

  val dataset = {
    val metadata = Metadata(id"test")
    
    val model = Function(
      Scalar(Metadata("id" -> "time", "type" -> "long")),
      Scalar(Metadata("id" -> "flux", "type" -> "double"))
    )
    
    val data = {
      val samples = List(
        Sample(DomainData(1l), RangeData(1.2d)),
        Sample(DomainData(2l), RangeData(2.4d))
      )
      SampledFunction(samples)
    }
    
    new TappedDataset(metadata, model, data)
  }
  
  "A dataset" should "have a string representation" in {
    dataset.toString should be ("test: time -> flux")
  }
  
  it should "provide a sample" in {
    StreamUtils.unsafeHead(dataset.samples) match {
      case Sample(DomainData(lv: Data.LongValue), RangeData(dv: Data.DoubleValue)) =>
        lv.value should be (1l)
        dv.value should be (1.2d)
    }
  }
  
  it should "apply an operation" in {
    val select = Selection("time", ">", "1")
    val ds2 = dataset.withOperation(select)
    StreamUtils.unsafeHead(ds2.samples) match {
      case Sample(DomainData(lv: Data.LongValue), RangeData(dv: Data.DoubleValue)) =>
        lv.value should be (2l)
        dv.value should be (2.4d)
    }
  }

  it should "read text data given an fdml" in {
    val textDs: Dataset = Dataset.fromName("data")
    textDs shouldBe a [Dataset]
  }

  it should "read matrix data given an fdml" in {
    val matrixDs: Dataset = Dataset.fromName("matrixData")
    matrixDs shouldBe a [Dataset]
  }
  
}

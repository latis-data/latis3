package latis.tests

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data._
import latis.dataset._
import latis.metadata.Metadata
import latis.model._
import latis.util.CacheManager
import latis.util.Identifier.IdentifierStringContext

class DatasetResolverSpec extends FlatSpec {
  
  val dataset = {
    val md = Metadata(id"data")
    // Note, there is an fdml dataset with the same name
    // but different structure.
        
    val model = Function(
      Scalar(Metadata("id" -> "myDomain", "type" -> "int")),
      Scalar(Metadata("id" -> "myRange", "type" -> "int"))
    )
    
    val data = {
      val samples = List(
        Sample(DomainData(1), RangeData(2))
      )
      SampledFunction(samples)
    }

    new TappedDataset(md, model, data)
  }

  "The DatasetResolver" should "find a cached dataset before all others" in {
    dataset.cache()
    Dataset.fromName(id"data").model match {
      case Function(d, _) =>
        d.id.get.asString should be ("myDomain")
    }
    CacheManager.removeDataset(id"data")
  }
  
  it should "find a dataset if not cached" in {
    Dataset.fromName(id"data").model match {
      case Function(d, _) =>
        d.id.get.asString should be ("time")
    }
  }
}

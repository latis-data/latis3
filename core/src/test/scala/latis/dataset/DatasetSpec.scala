package latis.dataset

import java.nio.file.Paths

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside

import latis.catalog.FdmlCatalog
import latis.data.Data._
import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.ops.Selection
import latis.util.Identifier.IdentifierStringContext
import latis.util.StreamUtils
import latis.util.dap2.parser.ast

class DatasetSpec extends AnyFlatSpec {

  val catalog = FdmlCatalog.fromClasspath(
    getClass().getClassLoader(),
    Paths.get("datasets"),
    validate = false
  )

  val dataset = {
    val metadata = Metadata(id"test")
    
    val model = Function(
      Scalar(Metadata("id" -> "time", "type" -> "long")),
      Scalar(Metadata("id" -> "flux", "type" -> "double"))
    )
    
    val data = {
      val samples = List(
        Sample(DomainData(1L), RangeData(1.2D)),
        Sample(DomainData(2L), RangeData(2.4D))
      )
      SampledFunction(samples)
    }
    
    new TappedDataset(metadata, model, data)
  }
  
  "A dataset" should "have a string representation" in {
    dataset.toString should be ("test: time -> flux")
  }
  
  it should "provide a sample" in {
    inside(StreamUtils.unsafeHead(dataset.samples)) {
      case Sample(DomainData(lv: Data.LongValue), RangeData(dv: Data.DoubleValue)) =>
        lv.value should be (1L)
        dv.value should be (1.2D)
    }
  }
  
  it should "apply an operation" in {
    val select = Selection(id"time", ast.Gt, "1")
    val ds2 = dataset.withOperation(select)
    inside(StreamUtils.unsafeHead(ds2.samples)) {
      case Sample(DomainData(lv: Data.LongValue), RangeData(dv: Data.DoubleValue)) =>
        lv.value should be (2L)
        dv.value should be (2.4D)
    }
  }

  it should "read text data given an fdml" in {
    val textDs: Dataset = catalog.findDataset(id"data").unsafeRunSync().getOrElse {
      fail("Unable to find dataset")
    }
    textDs shouldBe a [Dataset]
  }

  it should "read matrix data given an fdml" in {
    val matrixDs: Dataset = catalog.findDataset(id"matrixData").unsafeRunSync().getOrElse {
      fail("Unable to find dataset")
    }
    matrixDs shouldBe a [Dataset]
  }
  
}

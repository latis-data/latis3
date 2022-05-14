package latis.dataset

import fs2.io.file.Path
import munit.CatsEffectSuite

import latis.catalog.FdmlCatalog
import latis.data.Data._
import latis.data._
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.ops.Selection
import latis.util.Identifier.IdentifierStringContext
import latis.util.dap2.parser.ast

class DatasetSuite extends CatsEffectSuite {

  val catalog = FdmlCatalog.fromClasspath(
    getClass.getClassLoader,
    Path("datasets"),
    validate = false
  ).unsafeRunSync()

  val dataset = {
    val metadata = Metadata(id"test")

    val model = ModelParser.unsafeParse("time: long -> flux: double")
    
    val data = {
      val samples = List(
        Sample(DomainData(1L), RangeData(1.2D)),
        Sample(DomainData(2L), RangeData(2.4D))
      )
      SampledFunction(samples)
    }
    
    new TappedDataset(metadata, model, data)
  }
  
  test("have a string representation") {
    assertEquals(dataset.toString, "test: time -> flux")
  }
  
  test("provide a sample") {
    dataset.samples.compile.toList.map { lst =>
      lst.head match {
        case Sample(DomainData(lv: Data.LongValue), RangeData(dv: Data.DoubleValue)) =>
          assertEquals(lv.value, 1L)
          assertEquals(dv.value, 1.2D)
        case _ => fail("sample of correct type not generated")
      }
    }
  }
  
  test("apply an operation") {
    val select = Selection(id"time", ast.Gt, "1")
    val ds2 = dataset.withOperation(select)

    ds2.samples.compile.toList.map { lst =>
      lst.head match {
        case Sample(DomainData(lv: Data.LongValue), RangeData(dv: Data.DoubleValue)) =>
          assertEquals(lv.value, 2L)
          assertEquals(dv.value, 2.4D)
        case _ => fail("sample of correct type not generated")
      }
    }
  }

  test("read text data given an fdml") {
    catalog.findDataset(id"data").map { ds =>
      val textDs = ds.getOrElse(fail("Unable to find dataset"))
      assert(textDs.isInstanceOf[Dataset])
    }
  }

  test("read matrix data given an fdml") {
    catalog.findDataset(id"matrixData").map { ds =>
      val matrixDs = ds.getOrElse(fail("Unable to find dataset"))
      assert(matrixDs.isInstanceOf[Dataset])
    }
  }
  
}

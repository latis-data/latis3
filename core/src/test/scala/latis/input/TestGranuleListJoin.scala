package latis.input

import latis.model._
import latis.ops.GranuleListJoin
import latis.metadata.Metadata
import latis.data._
import latis.util.StreamUtils._
import cats.effect.IO
import fs2._
import latis.output._
import java.io.FileOutputStream

import org.junit._
import org.junit.Assert._
import latis.ops.Selection
import latis.ops.Projection
import latis.dataset.UnadaptedDataset
import latis.dataset.Dataset
import latis.util.StreamUtils

class TestGranuleListJoin {
  //TODO: ScalaTest flat spec?
  
  @Test
  def test() = {
    //granule list dataset: i -> uri
    val gl: Dataset = {
      val md = Metadata("test_dataset")
      val model = Function(
        Scalar(Metadata("id" -> "i", "type" -> "int")),
        Scalar(Metadata("id" -> "uri", "type" -> "string"))
      )
      val data = SampledFunction(
        //TODO: generate test data files
        Sample(DomainData(0), RangeData(s"file://${System.getProperty("user.home")}/git/latis3/core/src/test/resources/data/data.txt")),
        Sample(DomainData(1), RangeData(s"file://${System.getProperty("user.home")}/git/latis3/core/src/test/resources/data/data2.txt"))
      )
      new UnadaptedDataset(md, model, data)
    }
    
    //model for granule: a -> (b, c, d)
    def model: DataType = Function(
      Scalar(Metadata("id" -> "a", "type" -> "short")),
      Tuple(
        Scalar(Metadata("id" -> "b", "type" -> "int")),
        Scalar(Metadata("id" -> "c", "type" -> "float")), 
        Scalar(Metadata("id" -> "d", "type" -> "string"))
      )
    )
    
    val config: AdapterConfig = AdapterConfig {
      "className" -> "latis.input.TextAdapter"
    }    
    //val adapter = TextAdapter(model)
    
    val glj = GranuleListJoin(model, config)
    //val glj = GranuleListJoin(model, adapter)
    
//    val ops = Seq(
//      Selection("a", ">=", "2"),
//      Selection("a", "<=", "3"),
//      //Projection("a,b,d") //TODO: projection not working
//    )
    
    val ds = gl.withOperation(glj)
               .withOperation(Selection("a", ">=", "2"))
               .withOperation(Selection("a", "<=", "3"))
               //.withOperation(Projection("a,b,d"))
    
    //val ds = ops.foldLeft(glj(gl))((ds, op) => op(ds))
    //val ds = ops.foldLeft(gl.withOperation(glj))((ds, op) => op(ds))
    //val out = System.out //new FileOutputStream("/data/tmp/data3.txt")
    //TextWriter(System.out).write(ds)
    //val samples = ds.data.unsafeForce.samples
    val samples = StreamUtils.unsafeStreamToSeq(ds.samples)
    assertEquals(2, samples.length)
    samples(0) match {
      case Sample(DomainData(Integer(a)), RangeData(Integer(b), Real(c), Text(d))) =>
        assertEquals(2, a)
        assertEquals(4, b)
        assertEquals(3.3f, c, 0)
        assertEquals("c", d)
    }
    samples(1) match {
      case Sample(DomainData(Integer(a)), RangeData(Integer(b), Real(c), Text(d))) =>
        assertEquals(3, a)
        assertEquals(6, b)
        assertEquals(4.4f, c, 0)
        assertEquals("d", d)
    }
  }
}
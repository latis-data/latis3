package latis.input

import munit.FunSuite

//import latis.data._
//import latis.dataset._
//import latis.metadata.Metadata
//import latis.model._
//import latis.ops.GranuleListJoin
//import latis.ops.Selection
//import latis.util.Identifier._
//import latis.util.StreamUtils
//import latis.util.dap2.parser.ast

class GranuleListJoinSuite extends FunSuite {

//  ignore("granule list join") {
//    //granule list dataset: i -> uri
//    val gl: Dataset = {
//      val md = Metadata(id"test_dataset")
//      val model = Function(
//        Scalar(Metadata("id" -> "i", "type" -> "int")),
//        Scalar(Metadata("id" -> "uri", "type" -> "string"))
//      )
//      val data = SampledFunction(Seq(
//        //TODO: generate test data files
//        //github action: /home/runner/work/latis3/latis3/core/core/src/test/resources/data/data.txt (No such file or directory)
//        Sample(DomainData(0), RangeData(s"file://${System.getProperty("user.dir")}/core/src/test/resources/data/data.txt")),
//        Sample(DomainData(1), RangeData(s"file://${System.getProperty("user.dir")}/core/src/test/resources/data/data2.txt"))
//      ))
//      new TappedDataset(md, model, data)
//    }
//
//    //model for granule: a -> (b, c, d)
//    def model: DataType = Function(
//      Scalar(Metadata("id" -> "a", "type" -> "short")),
//      Tuple(
//        Scalar(Metadata("id" -> "b", "type" -> "int")),
//        Scalar(Metadata("id" -> "c", "type" -> "float")),
//        Scalar(Metadata("id" -> "d", "type" -> "string"))
//      )
//    )
//
//    val config: AdapterConfig = AdapterConfig {
//      "class" -> "latis.input.TextAdapter"
//    }
//    //val adapter = TextAdapter(model)
//
//    val glj = GranuleListJoin(model, config)
//    //val glj = GranuleListJoin(model, adapter)
//
////    val ops = Seq(
////      Selection("a", ">=", "2"),
////      Selection("a", "<=", "3"),
////      //Projection("a,b,d") //TODO: projection not working
////    )
//
//    val ds = gl.withOperation(glj)
//               .withOperation(Selection(id"a", ast.GtEq, "2"))
//               .withOperation(Selection(id"a", ast.LtEq, "3"))
//               //.withOperation(Projection("a,b,d"))
//
//    //val ds = ops.foldLeft(glj(gl))((ds, op) => op(ds))
//    //val ds = ops.foldLeft(gl.withOperation(glj))((ds, op) => op(ds))
//    //val out = System.out //new FileOutputStream("/data/tmp/data3.txt")
//    //val samples = ds.data.unsafeForce.samples
//    val samples = StreamUtils.unsafeStreamToSeq(ds.samples)
//    assert(samples.length == 2)
//    inside(samples.head) {
//      case Sample(DomainData(Integer(a)), RangeData(Integer(b), Real(c), Text(d))) =>
//        assert(a == 2)
//        assert(b == 4)
//        c should be(3.3 +- 0.001)
//        assert(d == "c")
//    }
//    inside(samples(1)) {
//      case Sample(DomainData(Integer(a)), RangeData(Integer(b), Real(c), Text(d))) =>
//        assert(a == 3)
//        assert(b == 6)
//        c should be(4.4 +- 0.001)
//        assert(d == "d")
//    }
//  }
}

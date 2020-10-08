package latis.input

import java.io.File

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import ucar.ma2.Section
import ucar.nc2.dataset.NetcdfDataset

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model._
import latis.output.NetcdfEncoder

class NetcdfWrapperSpec extends FlatSpec {

  private val mock2d: MemoizedDataset = {
    val samples = Seq(
      Sample(DomainData(1, "1"), RangeData(1.1, 0.1)),
      Sample(DomainData(1, "2"), RangeData(1.2, 0.2)),
      Sample(DomainData(2, "1"), RangeData(2.1, 0.3)),
      Sample(DomainData(2, "2"), RangeData(2.2, 0.4)),
      Sample(DomainData(3, "1"), RangeData(3.1, 0.5)),
      Sample(DomainData(3, "2"), RangeData(3.2, 0.6))
    )

    val md = Metadata("mock2d")
    val model = Function(
      Tuple(
        Scalar(Metadata("_1") + ("type" -> "int")),
        Scalar(Metadata("_2") + ("type" -> "string"))
      ),
      Tuple(
        Scalar(Metadata("a") + ("type" -> "double")),
        Scalar(Metadata("b") + ("type" -> "double"))
      )
    )
    val data = SampledFunction(samples)

    new MemoizedDataset(md, model, data)
  }

  private def makeSections(s: String): List[Section] =
    s.split(';').toList.map(new Section(_))

  private def makeConfig(s: String): NetcdfAdapter.Config =
    NetcdfAdapter.Config(("section", s))

  //-- Tests -----------------------------------------------------------------//

  "A NetcdfWrapper" should "create Sections from the adapter config string" in {
    val enc = NetcdfEncoder(File.createTempFile("netcdf_test", ".nc", null))
    val file = enc.encode(mock2d).compile.toList.unsafeRunSync().head
    val netcdfDataset = NetcdfDataset.openDataset(file.toString)
    val config2d = makeConfig("0:2; 0:1; 0:2, 0:1; 0:2, 0:1")
    val wrapper = NetcdfWrapper(netcdfDataset, mock2d.model, config2d)
    wrapper.sections should be(makeSections("0:2; 0:1; 0:2, 0:1; 0:2, 0:1"))
  }

  it should "replace null ranges in the adapter config string" in {
    val enc = NetcdfEncoder(File.createTempFile("netcdf_test", ".nc", null))
    val file = enc.encode(mock2d).compile.toList.unsafeRunSync().head
    val netcdfDataset = NetcdfDataset.openDataset(file.toString)
    val config2d = makeConfig(":; 0:1; :, 0:1; :, 0:1")
    val wrapper = NetcdfWrapper(netcdfDataset, mock2d.model, config2d)
    wrapper.sections should be(makeSections("0:2; 0:1; 0:2, 0:1; 0:2, 0:1"))
  }

}

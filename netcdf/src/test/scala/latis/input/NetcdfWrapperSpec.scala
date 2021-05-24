package latis.input

import java.io.File

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import ucar.ma2.Section
import ucar.nc2.dataset.NetcdfDataset

import latis.dataset.MemoizedDataset
import latis.output.NetcdfEncoder
import latis.dsl.DatasetGenerator

class NetcdfWrapperSpec extends AnyFlatSpec {

  private val mock2d: MemoizedDataset =
    DatasetGenerator("(a, b) -> (c, d)")

  private def makeSections(s: String): List[Section] =
    s.split(';').toList.map(new Section(_))

  private def makeConfig(s: String): NetcdfAdapter.Config =
    NetcdfAdapter.Config(("section", s))

  //-- Tests -----------------------------------------------------------------//

  "A NetcdfWrapper" should "create Sections from the adapter config string" in {
    val enc = NetcdfEncoder(File.createTempFile("netcdf_test", ".nc", null))
    val file = enc.encode(mock2d).compile.toList.unsafeRunSync().head
    val netcdfDataset = NetcdfDataset.openDataset(file.toString)
    val config2d = makeConfig("0:1; 0:2; 0:1, 0:2; 0:1, 0:2")
    val wrapper = NetcdfWrapper(netcdfDataset, mock2d.model, config2d)
    wrapper.sections should be(makeSections("0:1; 0:2; 0:1, 0:2; 0:1, 0:2"))
  }

  it should "replace null ranges in the adapter config string" in {
    val enc = NetcdfEncoder(File.createTempFile("netcdf_test", ".nc", null))
    val file = enc.encode(mock2d).compile.toList.unsafeRunSync().head
    val netcdfDataset = NetcdfDataset.openDataset(file.toString)
    val config2d = makeConfig(":; :; :, :; :, :")
    val wrapper = NetcdfWrapper(netcdfDataset, mock2d.model, config2d)
    wrapper.sections should be(makeSections("0:1; 0:2; 0:1, 0:2; 0:1, 0:2"))
  }

}

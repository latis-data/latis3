package latis.input

import java.net.URI

import munit.CatsEffectSuite

import latis.data.*
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.model.*
import latis.ops.*

class NetcdfAdapterSuite extends CatsEffectSuite {

  test("read data") {
    val uri = new URI("https://lasp.colorado.edu/eve/data_access/evewebdata/misc/oneau/LASP_dailynoonUTC_1aufactors_VSOP87_full.nc")
    val model = ModelParser.unsafeParse("time: double -> au_factor: double")
    val ops = List(Head())

    new NetcdfAdapter(model).getData(uri, ops).samples.map {
      case Sample(DomainData(Number(t)), RangeData(Number(v))) =>
        assertEquals(t, 2378497.0)
        assertEquals(v, 1.0344235109374846)
      case _ => fail("")
    }.compile.drain
  }

  test("implicit nc source id with escaped dots") {
    val s = Scalar.fromMetadata(Metadata(
      "id" -> "foo",
      "type" -> "int",
      "sourceId" -> "foo.bar"
    )).getOrElse(fail("failed to create scalar"))
    assertEquals(s.ncName, raw"foo\.bar")
  }

}

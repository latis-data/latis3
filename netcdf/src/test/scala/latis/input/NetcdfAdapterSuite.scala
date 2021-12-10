package latis.input

import java.net.URI

import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.EitherValues._
import org.scalatest.Inside.inside

import latis.data._
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.model._
import latis.ops._

class NetcdfAdapterSuite extends AnyFunSuite {

  test("read data") {
    val uri = new URI("https://lasp.colorado.edu/eve/data_access/evewebdata/misc/oneau/LASP_dailynoonUTC_1aufactors_VSOP87_full.nc")
    val model = ModelParser.unsafeParse("time: double -> au_factor: double")
    val ops = List(Head() )

    new NetcdfAdapter(model).getData(uri, ops).samples.map { s =>
      inside (s) {
        case Sample(DomainData(Number(t)), RangeData(Number(v))) =>
          assert(t == 2378497.0)
          assert(v == 1.0344235109374846)
      }
    }.compile.drain.unsafeRunSync()
  }

  test("implicit nc source id with escaped dots") {
    val s = Scalar.fromMetadata(Metadata(
      "id" -> "foo",
      "type" -> "int",
      "sourceId" -> "foo.bar"
    )).value
    assert(s.ncName == raw"foo\.bar")
  }

}

package latis.output

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import fs2.io.file.Files
import munit.CatsEffectSuite
import ucar.ma2.{Array as NcArray}
import ucar.ma2.{DataType as NcDataType}
import ucar.nc2.NetcdfFiles
import ucar.nc2.Variable

import latis.data.*
import latis.dataset.MemoizedDataset
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.model.*
import latis.util.Identifier.*

class NetcdfEncoderSuite extends CatsEffectSuite {
  import NetcdfEncoderSuite.*

  private val tempFile =
    ResourceFunFixture(Files[IO].tempFile(None, "netcdf_test", ".nc", None))

  private def readArrays(vars: List[Variable]): IO[List[NcArray]] =
    vars.traverse(v => IO.blocking(v.read()))

  tempFile.test("encode a 1-D dataset with a single range variable to NetCDF") { file =>
    val enc          = NetcdfEncoder(file)
    val expectedTime = List(1, 2, 3)
    val expectedFlux = List(1.0, 2.5, 5.1e-2)

    enc.encode(time_series_1D).compile.onlyOrError.flatMap { file =>
      Resource.fromAutoCloseable(
        IO(NetcdfFiles.open(file.absolute.toString))
      ).use { ncFile =>
        val vars = ncFile.getVariables.toArray.toList.asInstanceOf[List[Variable]]
        readArrays(vars).flatMap { arrs =>
          IO {
            val timeArr = arrs(0).get1DJavaArray(NcDataType.INT).asInstanceOf[Array[Int]]
            val fluxArr = arrs(1).get1DJavaArray(NcDataType.DOUBLE).asInstanceOf[Array[Double]]

            assertEquals(timeArr.toList, expectedTime)
            assertEquals(fluxArr.toList, expectedFlux)
          }
        }
      }
    }
  }

  tempFile.test("encode a 1-D dataset with multiple range variables to NetCDF") { file =>
    val enc          = NetcdfEncoder(file)
    val expectedTime = List(1, 2, 3)
    val expectedFlag = List[Byte](0, 0, -1)
    val expectedFlux = List(1.0, 2.5, 5.1e-2)
    val expectedLong = List[Long](9001, 9001, 9001)
    val expectedStr  = List("foo", "bar", "baz")

    enc.encode(time_series_1D_multi_range).compile.onlyOrError.flatMap { file =>
      Resource.fromAutoCloseable(
        IO(NetcdfFiles.open(file.absolute.toString))
      ).use { ncFile =>
        IO {
          assertEquals(
            ncFile.readSection("time").get1DJavaArray(NcDataType.INT).asInstanceOf[Array[Int]].toList,
            expectedTime
          )

          assertEquals(
            ncFile.readSection("flag").get1DJavaArray(NcDataType.BYTE).asInstanceOf[Array[Byte]].toList,
            expectedFlag
          )

          assertEquals(
            ncFile.readSection("flux").get1DJavaArray(NcDataType.DOUBLE).asInstanceOf[Array[Double]].toList,
            expectedFlux
          )

          assertEquals(
            ncFile.readSection("long").get1DJavaArray(NcDataType.LONG).asInstanceOf[Array[Long]].toList,
            expectedLong
          )

          assertEquals(
            ncFile.readSection("str").get1DJavaArray(NcDataType.STRING).asInstanceOf[Array[Any]].toList,
            expectedStr
          )
        }
      }
    }
  }

  tempFile.test("encode a 2-D dataset to NetCDF") { file =>
    val enc                = NetcdfEncoder(file)
    val expectedTime       = List(1, 2, 3)
    val expectedWavelength = List(430.1, 538.5)
    val expectedFlag       = List[Byte](0, 0, 0, -1, 0, 0)
    val expectedFlux       = List(1.0, 2.5, 1.2, 5.1e-2, 0.9, 2.1)

    enc.encode(time_series_2D).compile.onlyOrError.flatMap { file =>
      Resource.fromAutoCloseable(
        IO(NetcdfFiles.open(file.absolute.toString))
      ).use { ncFile =>
        IO {
          assertEquals(
            ncFile.readSection("time").get1DJavaArray(NcDataType.INT).asInstanceOf[Array[Int]].toList,
            expectedTime
          )

          assertEquals(
            ncFile.readSection("wavelength").get1DJavaArray(NcDataType.DOUBLE).asInstanceOf[Array[Double]].toList,
            expectedWavelength
          )

          assertEquals(
            ncFile.readSection("flag").get1DJavaArray(NcDataType.BYTE).asInstanceOf[Array[Byte]].toList,
            expectedFlag
          )

          assertEquals(
            ncFile.readSection("flux").get1DJavaArray(NcDataType.DOUBLE).asInstanceOf[Array[Double]].toList,
            expectedFlux
          )
        }
      }
    }
  }

  tempFile.test("encode a 3-D dataset to NetCDF") { file =>
    val enc                = NetcdfEncoder(file)
    val expectedTime       = List(1, 2)
    val expectedWavelength = List(430.1, 538.5)
    val expectedAnother    = List(1.1, 2.2)
    val expectedFlux       = List(1.0, 2.5, 1.2, 5.1e-2, 0.9, 2.1, 0.9, 2.1)

    enc.encode(time_series_3D).compile.onlyOrError.flatMap { file =>
      Resource.fromAutoCloseable(
        IO(NetcdfFiles.open(file.absolute.toString))
      ).use { ncFile =>
        IO {
          assertEquals(
            ncFile.readSection("time").get1DJavaArray(NcDataType.INT).asInstanceOf[Array[Int]].toList,
            expectedTime
          )

          assertEquals(
            ncFile.readSection("wavelength").get1DJavaArray(NcDataType.DOUBLE).asInstanceOf[Array[Double]].toList,
            expectedWavelength
          )

          assertEquals(
            ncFile.readSection("another").get1DJavaArray(NcDataType.DOUBLE).asInstanceOf[Array[Double]].toList,
            expectedAnother
          )

          assertEquals(
            ncFile.readSection("flux").get1DJavaArray(NcDataType.DOUBLE).asInstanceOf[Array[Double]].toList,
            expectedFlux
          )
        }
      }
    }
  }

  tempFile.test("include global metadata in the file") { file =>
    val enc              = NetcdfEncoder(file)
    val expectedMetadata = Metadata(id"dataset_with_metadata") + ("globalFoo" -> "globalBar")

    enc.encode(dataset_with_metadata).compile.onlyOrError.flatMap { file =>
      Resource.fromAutoCloseable(
        IO(NetcdfFiles.open(file.absolute.toString))
      ).use { ncFile =>
        IO {
          expectedMetadata.properties.foreach {
            case (k, v) => assertEquals(ncFile.findGlobalAttribute(k).getStringValue, v)
          }
        }
      }
    }
  }

  tempFile.test("include variable metadata in the file") { file =>
    val enc                  = NetcdfEncoder(file)
    val expectedTimeMetadata = Metadata(id"time") + ("type" -> "int") + ("scalarFoo" -> "scalarBar")
    val expectedFluxMetadata = Metadata(id"flux") + ("type" -> "double") + ("Foo" -> "Bar")

    enc.encode(dataset_with_metadata).compile.onlyOrError.flatMap { file =>
      Resource.fromAutoCloseable(
        IO(NetcdfFiles.open(file.absolute.toString))
      ).use { ncFile =>
        IO {
          val timeVar = ncFile.findVariable("time")
          expectedTimeMetadata.properties.foreach {
            case (k, v) => assertEquals(timeVar.findAttribute(k).getStringValue, v)
          }

          val fluxVar = ncFile.findVariable("flux")
          expectedFluxMetadata.properties.foreach {
            case (k, v) => assertEquals(fluxVar.findAttribute(k).getStringValue, v)
          }
        }
      }
    }
  }
}

object NetcdfEncoderSuite {

  private val time_series_1D: MemoizedDataset = {
    val samples = Seq(
      Sample(DomainData(1), RangeData(1.0)),
      Sample(DomainData(2), RangeData(2.5)),
      Sample(DomainData(3), RangeData(5.1e-2))
    )

    val md = Metadata(id"time_series_1D")
    val model = ModelParser.unsafeParse("time: int -> flux: double")
    val data = SampledFunction(samples)

    new MemoizedDataset(md, model, data)
  }

  private val time_series_1D_multi_range: MemoizedDataset = {
    val samples = Seq(
      Sample(DomainData(1), RangeData(0: Byte, 1.0, 9001: Long, "foo")),
      Sample(DomainData(2), RangeData(0: Byte, 2.5, 9001: Long, "bar")),
      Sample(DomainData(3), RangeData(-1: Byte, 5.1e-2, 9001: Long, "baz"))
    )

    val md = Metadata(id"time_series_1D_multi_range")
    val model = ModelParser.unsafeParse("time: int -> (flag: byte, flux: double, long: long, str: string)")
//      Function(
//      Scalar(Metadata(id"time") + ("type" -> "int")),
//      Tuple(
//        Scalar(Metadata(id"flag") + ("type" -> "byte")),
//        Scalar(Metadata(id"flux") + ("type" -> "double")),
//        Scalar(Metadata(id"long") + ("type" -> "long")),
//        Scalar(Metadata(id"str") + ("type"  -> "string"))
//      )
//    )
    val data = SampledFunction(samples)

    new MemoizedDataset(md, model, data)
  }

  private val time_series_2D: MemoizedDataset = {
    val samples = Seq(
      Sample(
        DomainData(1),
        RangeData(
          SampledFunction(
            Seq(
              Sample(DomainData(430.1), RangeData(0: Byte, 1.0)),
              Sample(DomainData(538.5), RangeData(0: Byte, 2.5))
            )
          )
        )
      ),
      Sample(
        DomainData(2),
        RangeData(
          SampledFunction(
            Seq(
              Sample(DomainData(430.1), RangeData(0: Byte, 1.2)),
              Sample(DomainData(538.5), RangeData(-1: Byte, 5.1e-2))
            )
          )
        )
      ),
      Sample(
        DomainData(3),
        RangeData(
          SampledFunction(
            Seq(
              Sample(DomainData(430.1), RangeData(0: Byte, 0.9)),
              Sample(DomainData(538.5), RangeData(0: Byte, 2.1))
            )
          )
        )
      )
    )

    val md = Metadata(id"time_series_2D")
    val model = ModelParser.unsafeParse("time: int -> wavelength: double -> (flag: byte, flux: double)")
//      Function(
//      Scalar(Metadata(id"time") + ("type" -> "int")),
//      Function(
//        Scalar(Metadata(id"wavelength") + ("type" -> "double")),
//        Tuple(
//          Scalar(Metadata(id"flag") + ("type" -> "byte")),
//          Scalar(Metadata(id"flux") + ("type" -> "double"))
//        )
//      )
//    )
    val data = SampledFunction(samples)

    new MemoizedDataset(md, model, data)
  }

  private val time_series_3D: MemoizedDataset = {
    val samples = Seq(
      Sample(DomainData(1, 430.1, 1.1), RangeData(1.0)),
      Sample(DomainData(1, 430.1, 2.2), RangeData(2.5)),
      Sample(DomainData(1, 538.5, 1.1), RangeData(1.2)),
      Sample(DomainData(1, 538.5, 2.2), RangeData(5.1e-2)),
      Sample(DomainData(2, 430.1, 1.1), RangeData(0.9)),
      Sample(DomainData(2, 430.1, 2.2), RangeData(2.1)),
      Sample(DomainData(2, 538.5, 1.1), RangeData(0.9)),
      Sample(DomainData(2, 538.5, 2.2), RangeData(2.1))
    )

    val md = Metadata(id"time_series_3D")
    val model = ModelParser.unsafeParse("(time: int, wavelength: double, another: double) -> flux: double")
//      Function(
//      Tuple(
//        Scalar(Metadata(id"time") + ("type"       -> "int")),
//        Scalar(Metadata(id"wavelength") + ("type" -> "double")),
//        Scalar(Metadata(id"another") + ("type"    -> "double"))
//      ),
//      Scalar(Metadata(id"flux") + ("type" -> "double"))
//    )
    val data = SampledFunction(samples)

    new MemoizedDataset(md, model, data)
  }

  private val dataset_with_metadata: MemoizedDataset = {
    val samples = Seq(
      Sample(DomainData(1), RangeData(1.0))
    )

    val md = Metadata(id"dataset_with_metadata") + ("globalFoo" -> "globalBar")
    val model = (
      Scalar.fromMetadata(Metadata(id"time") + ("type" -> "int") + ("scalarFoo" -> "scalarBar")),
      Scalar.fromMetadata(Metadata(id"flux") + ("type" -> "double") + ("Foo"    -> "Bar"))
    ).flatMapN(Function.from).fold(throw _, identity)
    val data = SampledFunction(samples)

    new MemoizedDataset(md, model, data)
  }
}

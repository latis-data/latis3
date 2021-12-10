package latis.output

import java.io.File
import java.nio.DoubleBuffer
import java.nio.IntBuffer

import cats.effect.unsafe.implicits.global
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import ucar.ma2.{DataType => NcDataType}
import ucar.nc2.NetcdfFiles

import latis.data._
import latis.dataset.MemoizedDataset
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.model._
import latis.util.Identifier.IdentifierStringContext

class NetcdfEncoderSpec extends AnyFlatSpec {
  import NetcdfEncoderSpec._

  "A NetCDF encoder" should "encode a 1-D dataset with a single range variable to NetCDF" in {
    val enc          = NetcdfEncoder(File.createTempFile("netcdf_test", ".nc", null))
    val expectedTime = Array(1, 2, 3)
    val expectedFlux = Array(1.0, 2.5, 5.1e-2)

    val file   = enc.encode(time_series_1D).compile.toList.unsafeRunSync().head
    val ncFile = NetcdfFiles.open(file.getAbsolutePath)
    try {
      val arrs    = ncFile.readArrays(ncFile.getVariables)
      val timeArr = arrs.get(0).getDataAsByteBuffer.asIntBuffer
      val fluxArr = arrs.get(1).getDataAsByteBuffer.asDoubleBuffer
      timeArr should be(IntBuffer.wrap(expectedTime))
      fluxArr should be(DoubleBuffer.wrap(expectedFlux))
    } finally {
      ncFile.close()
      val _ = file.delete()
    }
  }

  it should "encode a 1-D dataset with multiple range variables to NetCDF" in {
    val enc          = NetcdfEncoder(File.createTempFile("netcdf_test", ".nc", null))
    val expectedTime = Array(1, 2, 3)
    val expectedFlag = Array[Byte](0, 0, -1)
    val expectedFlux = Array(1.0, 2.5, 5.1e-2)
    val expectedLong = Array[Long](9001, 9001, 9001)
    val expectedStr  = Array("foo", "bar", "baz")

    val file   = enc.encode(time_series_1D_multi_range).compile.toList.unsafeRunSync().head
    val ncFile = NetcdfFiles.open(file.getAbsolutePath)
    try {
      ncFile.readSection("time").get1DJavaArray(NcDataType.INT) should be(expectedTime)
      ncFile.readSection("flag").get1DJavaArray(NcDataType.BYTE) should be(expectedFlag)
      ncFile.readSection("flux").get1DJavaArray(NcDataType.DOUBLE) should be(expectedFlux)
      ncFile.readSection("long").get1DJavaArray(NcDataType.LONG) should be(expectedLong)
      ncFile.readSection("str").get1DJavaArray(NcDataType.STRING) should be(expectedStr)
    } finally {
      ncFile.close()
      val _ = file.delete()
    }
  }

  it should "encode a 2-D dataset to NetCDF" in {
    val enc                = NetcdfEncoder(File.createTempFile("netcdf_test", ".nc", null))
    val expectedTime       = Array(1, 2, 3)
    val expectedWavelength = Array(430.1, 538.5)
    val expectedFlag       = Array[Byte](0, 0, 0, -1, 0, 0)
    val expectedFlux       = Array(1.0, 2.5, 1.2, 5.1e-2, 0.9, 2.1)

    val file   = enc.encode(time_series_2D).compile.toList.unsafeRunSync().head
    val ncFile = NetcdfFiles.open(file.getAbsolutePath)
    try {
      ncFile.readSection("time").get1DJavaArray(NcDataType.INT) should be(expectedTime)
      ncFile.readSection("wavelength").get1DJavaArray(NcDataType.DOUBLE) should be(
        expectedWavelength
      )
      ncFile.readSection("flag").get1DJavaArray(NcDataType.BYTE) should be(expectedFlag)
      ncFile.readSection("flux").get1DJavaArray(NcDataType.DOUBLE) should be(expectedFlux)
    } finally {
      ncFile.close()
      val _ = file.delete()
    }
  }

  it should "encode a 3-D dataset to NetCDF" in {
    val enc                = NetcdfEncoder(File.createTempFile("netcdf_test", ".nc", null))
    val expectedTime       = Array(1, 2)
    val expectedWavelength = Array(430.1, 538.5)
    val expectedAnother    = Array(1.1, 2.2)
    val expectedFlux       = Array(1.0, 2.5, 1.2, 5.1e-2, 0.9, 2.1, 0.9, 2.1)

    val file   = enc.encode(time_series_3D).compile.toList.unsafeRunSync().head
    val ncFile = NetcdfFiles.open(file.getAbsolutePath)
    try {
      ncFile.readSection("time").get1DJavaArray(NcDataType.INT) should be(expectedTime)
      ncFile.readSection("wavelength").get1DJavaArray(NcDataType.DOUBLE) should be(
        expectedWavelength
      )
      ncFile.readSection("another").get1DJavaArray(NcDataType.DOUBLE) should be(expectedAnother)
      ncFile.readSection("flux").get1DJavaArray(NcDataType.DOUBLE) should be(expectedFlux)
    } finally {
      ncFile.close()
      val _ = file.delete()
    }
  }

  it should "include global metadata in the file" in {
    val enc              = NetcdfEncoder(File.createTempFile("netcdf_test", ".nc", null))
    val expectedMetadata = Metadata(id"dataset_with_metadata") + ("globalFoo" -> "globalBar") + ("history" -> "Uncurry()")

    val file   = enc.encode(dataset_with_metadata).compile.toList.unsafeRunSync().head
    val ncFile = NetcdfFiles.open(file.getAbsolutePath)
    try {
      expectedMetadata.properties.foreach {
        case (k, v) => ncFile.findGlobalAttribute(k).getStringValue should be(v)
      }
    } finally {
      ncFile.close()
      val _ = file.delete()
    }
  }

  it should "include variable metadata in the file" in {
    val enc                  = NetcdfEncoder(File.createTempFile("netcdf_test", ".nc", null))
    val expectedTimeMetadata = Metadata(id"time") + ("type" -> "int") + ("scalarFoo" -> "scalarBar")
    val expectedFluxMetadata = Metadata(id"flux") + ("type" -> "double") + ("Foo" -> "Bar")

    val file   = enc.encode(dataset_with_metadata).compile.toList.unsafeRunSync().head
    val ncFile = NetcdfFiles.open(file.getAbsolutePath)
    try {
      val timeVar = ncFile.findVariable("time")
      expectedTimeMetadata.properties.foreach {
        case (k, v) => timeVar.findAttribute(k).getStringValue should be(v)
      }
      val fluxVar = ncFile.findVariable("flux")
      expectedFluxMetadata.properties.foreach {
        case (k, v) => fluxVar.findAttribute(k).getStringValue should be(v)
      }
    } finally {
      ncFile.close()
      val _ = file.delete()
    }
  }
}

object NetcdfEncoderSpec {

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
    val model = Function.from(
      Scalar.fromMetadata(Metadata(id"time") + ("type" -> "int") + ("scalarFoo" -> "scalarBar")).value,
      Scalar.fromMetadata(Metadata(id"flux") + ("type" -> "double") + ("Foo"    -> "Bar")).value
    ).value
    val data = SampledFunction(samples)

    new MemoizedDataset(md, model, data)
  }
}

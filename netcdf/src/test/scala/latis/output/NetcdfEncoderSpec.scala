package latis.output

import java.io.File
import java.nio.ByteBuffer
import java.nio.DoubleBuffer
import java.nio.IntBuffer
import java.nio.LongBuffer

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import ucar.nc2.NetcdfFile

import latis.data._
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model._

class NetcdfEncoderSpec extends FlatSpec {
  import NetcdfEncoderSpec._

  "A NetCDF encoder" should "encode a 1-D dataset with a single range variable to NetCDF" in {
    val enc          = NetcdfEncoder(new File("test1.nc"))
    val expectedTime = Array(1, 2, 3)
    val expectedFlux = Array(1.0, 2.5, 5.1e-2)

    val file   = enc.encode(time_series_1D).compile.toList.unsafeRunSync().head
    val ncFile = NetcdfFile.open(file.getAbsolutePath)
    try {
      val arrs    = ncFile.readArrays(ncFile.getVariables)
      val timeArr = arrs.get(0).getDataAsByteBuffer.asIntBuffer
      val fluxArr = arrs.get(1).getDataAsByteBuffer.asDoubleBuffer
      timeArr should be(IntBuffer.wrap(expectedTime))
      fluxArr should be(DoubleBuffer.wrap(expectedFlux))
    } finally {
      ncFile.close()
      file.delete()
    }
  }

  it should "encode a 1-D dataset with multiple range variables to NetCDF" in {
    val enc          = NetcdfEncoder(new File("test2.nc"))
    val expectedTime = Array(1, 2, 3)
    val expectedFlag = Array[Byte](0, 0, -1)
    val expectedFlux = Array(1.0, 2.5, 5.1e-2)
    val expectedLong = Array[Long](9001, 9001, 9001)
    val expectedStr  = "foo bar baz "

    val file   = enc.encode(time_series_1D_multi_range).compile.toList.unsafeRunSync().head
    val ncFile = NetcdfFile.open(file.getAbsolutePath)
    try {
      val arrs    = ncFile.readArrays(ncFile.getVariables)
      val timeArr = arrs.get(0).getDataAsByteBuffer.asIntBuffer
      val flagArr = arrs.get(1).getDataAsByteBuffer
      val fluxArr = arrs.get(2).getDataAsByteBuffer.asDoubleBuffer
      val longArr = arrs.get(3).getDataAsByteBuffer.asLongBuffer
      val strArr  = arrs.get(4).toString
      timeArr should be(IntBuffer.wrap(expectedTime))
      flagArr should be(ByteBuffer.wrap(expectedFlag))
      fluxArr should be(DoubleBuffer.wrap(expectedFlux))
      longArr should be(LongBuffer.wrap(expectedLong))
      strArr should be(expectedStr)
    } finally {
      ncFile.close()
      file.delete()
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

    val md = Metadata("time_series_1D")
    val model = Function(
      Scalar(Metadata("time") + ("type" -> "int")),
      Scalar(Metadata("flux") + ("type" -> "double"))
    )
    val data = SampledFunction(samples)

    new MemoizedDataset(md, model, data)
  }

  private val time_series_1D_multi_range: MemoizedDataset = {
    val samples = Seq(
      Sample(DomainData(1), RangeData(0: Byte, 1.0, 9001: Long, "foo")),
      Sample(DomainData(2), RangeData(0: Byte, 2.5, 9001: Long, "bar")),
      Sample(DomainData(3), RangeData(-1: Byte, 5.1e-2, 9001: Long, "baz"))
    )

    val md = Metadata("time_series_1D_multi_range")
    val model = Function(
      Scalar(Metadata("time") + ("type" -> "int")),
      Tuple(
        Scalar(Metadata("flag") + ("type" -> "byte")),
        Scalar(Metadata("flux") + ("type" -> "double")),
        Scalar(Metadata("long") + ("type" -> "long")),
        Scalar(Metadata("str") + ("type"  -> "string"))
      )
    )
    val data = SampledFunction(samples)

    new MemoizedDataset(md, model, data)
  }
}

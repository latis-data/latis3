package latis.output

import java.io.File

import cats.effect.IO
import cats.effect.Resource
import fs2.Stream
import ucar.ma2.{DataType => NcDataType}
import ucar.ma2.{Array => NcArray}
import ucar.nc2.Dimension
import ucar.nc2.NetcdfFileWriter
import ucar.nc2.NetcdfFileWriter.Version.netcdf4
import ucar.nc2.Variable

import latis.data.Data
import latis.data.Data._
import latis.data.Sample
import latis.dataset.Dataset
import latis.model._
import latis.model.DataType
import latis.ops.Uncurry
import latis.util.LatisException

/**
 * Makes a [[https://www.unidata.ucar.edu/software/netcdf/ NetCDF4]] file from a [[latis.dataset.Dataset]].
 *
 * This encoder assumes the dataset is uncurried.
 *
 * Throws a `LatisExeption` if the dataset includes any of the following types:
 *   - `Boolean`
 *   - `Binary`
 *   - `BigInteger`
 *   - `BigDecimal`
 *
 * @param file location to write to.
 */
class NetcdfEncoder(file: File) extends Encoder[IO, File] {
  import NetcdfEncoder._
  private val path = file.getAbsolutePath
  private val ncFileWriter: Resource[IO, NetcdfFileWriter] =
    Resource.fromAutoCloseable(IO(NetcdfFileWriter.createNew(netcdf4, path)))

  /**
   * Encodes a [[latis.dataset.Dataset]] to netCDF4
   * @param dataset dataset to encode
   */
  override def encode(dataset: Dataset): Stream[IO, File] = {
    val uncurriedDataset = dataset.withOperation(Uncurry())
    Stream
      .resource(ncFileWriter)
      .flatMap { ncdf =>
        Stream
          .eval(uncurriedDataset.samples.compile.toVector)
          .map(datasetToNetcdf(ncdf, uncurriedDataset.model, _))
      }
      .as(file)
  }

  private def datasetToNetcdf(
    file: NetcdfFileWriter,
    model: DataType,
    datasetList: Vector[Sample]
  ): Unit = model match {
    case Function(domain: Scalar, range) =>
      // add metadata (dimension(s) and variables)
      val dim                 = file.addDimension(domain.id, datasetList.length)
      val scalars             = domain +: range.getScalars
      val vars: Seq[Variable] = addVariablesFromScalars(file, dim, scalars)
      file.create() // create file and write metadata

      // write data
      val acc: Accumulator   = scalarsToAccumulator(scalars, datasetList.length)
      val data: Seq[NcArray] = accumulate(acc, scalars, datasetList)
      vars.zip(data).foreach { case (v, d) => file.write(v, d) }
    case _ =>
      throw LatisException(
        "dataset must be a function of a scalar to either a scalar or tuple of scalars"
      )
  }
}

object NetcdfEncoder {
  def apply(file: File): NetcdfEncoder =
    new NetcdfEncoder(file)

  private type Accumulator = List[Any]

  private def accumulate(acc: Accumulator, ss: Seq[Scalar], data: Seq[Sample]): Seq[NcArray] = {
    for (i <- data.indices) yield {
      data(i) match {
        case Sample(d, rs) =>
          val vals: Array[Data] = (d ++ rs).toArray
          (vals, ss, acc).zipped.toList.foreach {
            case (v, s, a) =>
              s.valueType match {
                case ByteValueType =>
                  a.asInstanceOf[Array[Byte]](i) = v.asInstanceOf[ByteValue].value
                case CharValueType =>
                  a.asInstanceOf[Array[Char]](i) = v.asInstanceOf[CharValue].value
                case ShortValueType =>
                  a.asInstanceOf[Array[Short]](i) = v.asInstanceOf[ShortValue].value
                case IntValueType => a.asInstanceOf[Array[Int]](i) = v.asInstanceOf[IntValue].value
                case LongValueType =>
                  a.asInstanceOf[Array[Long]](i) = v.asInstanceOf[LongValue].value
                case FloatValueType =>
                  a.asInstanceOf[Array[Float]](i) = v.asInstanceOf[FloatValue].value
                case DoubleValueType =>
                  a.asInstanceOf[Array[Double]](i) = v.asInstanceOf[DoubleValue].value
                case StringValueType =>
                  a.asInstanceOf[Array[String]](i) = v.asInstanceOf[StringValue].value
                case t => throw LatisException(s"Unsupported type: $t")
              }
          }
        case _ =>
          throw LatisException(
            "dataset must be a function of a scalar to either a scalar or tuple of scalars"
          )
      }
    }
    val shape: Array[Int] = Array(data.length)
    ss.zip(acc).map {
      case (s, a) =>
        s.valueType match {
          case ByteValueType   => NcArray.factory(scalarToNetcdfDataType(s), shape, a)
          case CharValueType   => NcArray.factory(scalarToNetcdfDataType(s), shape, a)
          case ShortValueType  => NcArray.factory(scalarToNetcdfDataType(s), shape, a)
          case IntValueType    => NcArray.factory(scalarToNetcdfDataType(s), shape, a)
          case LongValueType   => NcArray.factory(scalarToNetcdfDataType(s), shape, a)
          case FloatValueType  => NcArray.factory(scalarToNetcdfDataType(s), shape, a)
          case DoubleValueType => NcArray.factory(scalarToNetcdfDataType(s), shape, a)
          case StringValueType => NcArray.factory(scalarToNetcdfDataType(s), shape, a)
          case t               => throw LatisException(s"Unsupported type: $t")
        }
    }
  }

  private def scalarsToAccumulator(ss: Seq[Scalar], length: Int): Accumulator =
    ss.map(_.valueType)
      .map {
        case ByteValueType   => new Array[Byte](length)
        case CharValueType   => new Array[Char](length)
        case ShortValueType  => new Array[Short](length)
        case IntValueType    => new Array[Int](length)
        case LongValueType   => new Array[Long](length)
        case FloatValueType  => new Array[Float](length)
        case DoubleValueType => new Array[Double](length)
        case StringValueType => new Array[String](length)
        case t               => throw LatisException(s"Unsupported type: $t")
      }
      .toList

  private def scalarToNetcdfDataType(s: Scalar): NcDataType =
    s.metadata.getProperty("type", "").toLowerCase match {
      case "byte"   => NcDataType.BYTE
      case "char"   => NcDataType.CHAR
      case "short"  => NcDataType.SHORT
      case "int"    => NcDataType.INT
      case "long"   => NcDataType.LONG
      case "float"  => NcDataType.FLOAT
      case "double" => NcDataType.DOUBLE
      case "string" => NcDataType.STRING
      // Boolean is not supported by netCDF3
      case t => throw LatisException(s"Unsupported type: $t")
    }

  private def addVariablesFromScalars(
    file: NetcdfFileWriter,
    dim: Dimension,
    ss: Seq[Scalar]
  ): Seq[Variable] =
    ss.map { s =>
      file.addVariable(s.id, scalarToNetcdfDataType(s), dim.getShortName)
    }
}

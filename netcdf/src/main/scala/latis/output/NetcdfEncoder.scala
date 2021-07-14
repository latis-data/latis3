package latis.output

import java.io.File

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._
import fs2.Stream
import ucar.ma2.{Array => NcArray}
import ucar.ma2.{DataType => NcDataType}
import ucar.nc2.Attribute
import ucar.nc2.Dimension
import ucar.nc2.NetcdfFileWriter
import ucar.nc2.NetcdfFileWriter.Version.netcdf4
import ucar.nc2.Variable

import latis.data.Data
import latis.data.Data._
import latis.data.Sample
import latis.dataset.Dataset
import latis.metadata.Metadata
import latis.model.DataType
import latis.model._
import latis.ops.Uncurry
import latis.util.LatisException

/**
 * Makes a [[https://www.unidata.ucar.edu/software/netcdf/ NetCDF4]] file from a [[latis.dataset.Dataset]].
 *
 * This encoder assumes:
 *   - the dataset is Cartesian with no missing values
 *   - the dataset is sorted so that the first domain variable changes slowest
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
  //TODO: deal with NullData, require replaceMissing?
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
          .evalMap(datasetToNetcdf(ncdf, uncurriedDataset.model, uncurriedDataset.metadata, _))
      }
      .as(file)
  }

  private def datasetToNetcdf(
    file: NetcdfFileWriter,
    model: DataType,
    metadata: Metadata,
    samples: Vector[Sample]
  ): IO[Unit] = model match {
    case Function(domain, range) =>
      val dScalars = domain.getScalars
      val rScalars = range.getScalars
      val scalars  = dScalars ++ rScalars
      val dimNames = dScalars.map(_.id.asString).mkString(" ")
      val acc: Accumulator =
        accumulate(scalarsToAccumulator(scalars, samples.length), scalars, samples)
      val dArrs = domainAccumulatorToNcArray(acc, dScalars)
      val shape = dArrs.map(_.getSize.toInt).toArray
      val rArrs = accumulatorToNcArray(acc.drop(dScalars.length), rScalars, shape)

      for {
        // add dimensions
        _ <- dScalars.zip(shape).traverse { case (s, dim) =>
          val sId = s.id.asString
          addDimension(file, sId, dim)
        }
        // add variables
        dVars <- dScalars.traverse(s =>
          addVariable(
            file,
            s.id.asString,
            scalarToNetcdfDataType(s),
            s.id.asString
          )
        )
        rVars <- rScalars.traverse(
          s => addVariable(
            file,
            s.id.asString,
            scalarToNetcdfDataType(s),
            dimNames
          )
        )
        // add metadata
        _ <- metadata.properties.toList.traverse { case (k, v) => addGlobalAttribute(file, k, v) }
        _ <- (dVars ++ rVars).zip(scalars).traverse {
          case (variable, scalar) =>
            scalar.metadata.properties.toList.traverse {
              case (key, value) => addAttribute(variable, key, value)
            }
        }
        _ <- create(file) // create file and write metadata
        // write data
        _ <- (dVars ++ rVars).zip(dArrs ++ rArrs).traverse { case (v, d) => write(file, v, d) }
      } yield ()
    case _ => IO.raiseError(LatisException("NetcdfEncoder assumes data model is a Function"))
  }
}

object NetcdfEncoder {
  def apply(file: File): NetcdfEncoder =
    new NetcdfEncoder(file)

  private type Accumulator = List[Any]

  private def accumulate(acc: Accumulator, ss: Seq[Scalar], data: Seq[Sample]): Accumulator = {
    for (i <- data.indices) yield {
      data(i) match {
        // for each sample (row), add the domain and range values to the arrays
        // (columns) in the accumulator.
        case Sample(d, r) =>
          val vals: Array[Data] = (d ++ r).toArray
          vals.lazyZip(ss).lazyZip(acc).foreach {
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
      }
    }
    acc
  }

  /**
   * Removes duplicate values from the input accumulator before converting to a
   * NetCDF Array.
   *
   * Expects at least one input argument to be domain only.
   */
  private def domainAccumulatorToNcArray(acc: Accumulator, scalars: Seq[Scalar]): Seq[NcArray] =
    scalars.zip(acc).map {
      case (s, a) =>
        s.valueType match {
          case ByteValueType =>
            val b = a.asInstanceOf[Array[Byte]].distinct
            NcArray.factory(scalarToNetcdfDataType(s), Array(b.length), b)
          case CharValueType =>
            val b = a.asInstanceOf[Array[Char]].distinct
            NcArray.factory(scalarToNetcdfDataType(s), Array(b.length), b)
          case ShortValueType =>
            val b = a.asInstanceOf[Array[Short]].distinct
            NcArray.factory(scalarToNetcdfDataType(s), Array(b.length), b)
          case IntValueType =>
            val b = a.asInstanceOf[Array[Int]].distinct
            NcArray.factory(scalarToNetcdfDataType(s), Array(b.length), b)
          case LongValueType =>
            val b = a.asInstanceOf[Array[Long]].distinct
            NcArray.factory(scalarToNetcdfDataType(s), Array(b.length), b)
          case FloatValueType =>
            val b = a.asInstanceOf[Array[Float]].distinct
            NcArray.factory(scalarToNetcdfDataType(s), Array(b.length), b)
          case DoubleValueType =>
            val b = a.asInstanceOf[Array[Double]].distinct
            NcArray.factory(scalarToNetcdfDataType(s), Array(b.length), b)
          case StringValueType =>
            val b = a.asInstanceOf[Array[String]].distinct
            NcArray.factory(scalarToNetcdfDataType(s), Array(b.length), b)
          case t => throw LatisException(s"Unsupported type: $t")
        }
    }

  private def accumulatorToNcArray(
    acc: Accumulator,
    scalars: Seq[Scalar],
    shape: Array[Int]
  ): Seq[NcArray] =
    scalars.zip(acc).map {
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
    s.valueType match {
      case ByteValueType   => NcDataType.BYTE
      case CharValueType   => NcDataType.CHAR
      case ShortValueType  => NcDataType.SHORT
      case IntValueType    => NcDataType.INT
      case LongValueType   => NcDataType.LONG
      case FloatValueType  => NcDataType.FLOAT
      case DoubleValueType => NcDataType.DOUBLE
      case StringValueType => NcDataType.STRING
      // Boolean is not supported by netCDF4
      case t => throw LatisException(s"Unsupported type: $t")
    }

  private def addDimension(file: NetcdfFileWriter, name: String, length: Int): IO[Dimension] =
    IO { file.addDimension(name, length) }

  private def addVariable(
    file: NetcdfFileWriter,
    varName: String,
    ncType: NcDataType,
    dimName: String
  ): IO[Variable] =
    IO { file.addVariable(varName, ncType, dimName) }

  private def create(file: NetcdfFileWriter): IO[Unit] =
    IO { file.create() }

  private def write(file: NetcdfFileWriter, v: Variable, data: NcArray): IO[Unit] =
    IO { file.write(v, data) }

  private def addGlobalAttribute(
    file: NetcdfFileWriter,
    key: String,
    value: String
  ): IO[Attribute] =
    IO { file.addGlobalAttribute(key, value) }

  private def addAttribute(v: Variable, key: String, value: String): IO[Attribute] =
    IO { v.addAttribute(new Attribute(key, value)) }
}

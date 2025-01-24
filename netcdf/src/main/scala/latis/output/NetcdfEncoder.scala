package latis.output

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import fs2.Stream
import fs2.io.file.Path
import ucar.ma2.{Array as NcArray}
import ucar.ma2.{DataType as NcDataType}
import ucar.nc2.Attribute
import ucar.nc2.Dimension
import ucar.nc2.write.NetcdfFileFormat
import ucar.nc2.write.NetcdfFormatWriter

import latis.data.Data
import latis.data.Data.*
import latis.data.Sample
import latis.dataset.Dataset
import latis.metadata.Metadata
import latis.model.*
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
class NetcdfEncoder(file: Path) extends Encoder[IO, Path] {
  //TODO: deal with NullData, require replaceMissing?
  import NetcdfEncoder.*
  private val path = file.absolute.toString

  /**
   * Encodes a [[latis.dataset.Dataset]] to netCDF4
   * @param dataset dataset to encode
   */
  override def encode(dataset: Dataset): Stream[IO, Path] = {
    val uncurriedDataset = dataset.withOperation(Uncurry())
    Stream
      .eval {
        IO {
          NetcdfFormatWriter.createNewNetcdf4(
            NetcdfFileFormat.NETCDF4,
            path,
            null
          )
        }
      }
      .flatMap { builder =>
        Stream
          .eval(uncurriedDataset.samples.compile.toVector)
          .evalMap {
            datasetToNetcdf(
              builder,
              uncurriedDataset.model,
              uncurriedDataset.metadata,
              _
            )
          }
      }
      .as(file)
  }

  private def datasetToNetcdf(
    builder: NetcdfFormatWriter.Builder,
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
          addDimension(builder, sId, dim)
        }
        // add variables
        dVars <- dScalars.traverse(s =>
          addVariable(
            builder,
            s.id.asString,
            scalarToNetcdfDataType(s),
            s.id.asString,
            s.metadata.properties.toList
          )
        )
        rVars <- rScalars.traverse(
          s => addVariable(
            builder,
            s.id.asString,
            scalarToNetcdfDataType(s),
            dimNames,
            s.metadata.properties.toList
          )
        )
        // add global metadata
        _ <- metadata.properties.toList.traverse { case (k, v) => addGlobalAttribute(builder, k, v) }
        // write data
        writer = build(builder)
        _ <- writer.use { writer =>
          (dVars ++ rVars).zip(dArrs ++ rArrs).traverse { (v, d) =>
            write(writer, v, d)
          }
        }
      } yield ()
    case _ => IO.raiseError(LatisException("NetcdfEncoder assumes data model is a Function"))
  }
}

object NetcdfEncoder {
  def apply(file: Path): NetcdfEncoder =
    new NetcdfEncoder(file)

  private type Accumulator = List[Any]

  @annotation.nowarn("msg=unused value")
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

  private def addDimension(
    builder: NetcdfFormatWriter.Builder,
    name: String,
    length: Int
  ): IO[Dimension] =
    IO { builder.addDimension(name, length) }

  private def addVariable(
    builder: NetcdfFormatWriter.Builder,
    varName: String,
    ncType: NcDataType,
    dimName: String,
    metadata: List[(String, String)]
  ): IO[String] = IO {
    builder.addVariable(varName, ncType, dimName)
  }.flatMap { varBuilder =>
    metadata.traverseVoid { (k, v) =>
      IO(varBuilder.addAttribute(new Attribute(k, v)))
    }.as(varName)
  }

  private def build(
    builder: NetcdfFormatWriter.Builder
  ): Resource[IO, NetcdfFormatWriter] =
    Resource.fromAutoCloseable(IO.blocking(builder.build()))

  private def write(
    writer: NetcdfFormatWriter,
    varName: String,
    data: NcArray
  ): IO[Unit] = IO.blocking(writer.write(varName, data))

  private def addGlobalAttribute(
    builder: NetcdfFormatWriter.Builder,
    key: String,
    value: String
  ): IO[Unit] =
    IO { builder.addAttribute(new Attribute(key, value)) }.void
}

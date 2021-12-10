package latis.input

import java.net.URI

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._
import fs2._
import ucar.ma2.{Array => NcArray}
import ucar.ma2.{Section => NcSection}
import ucar.nc2.Dimension
import ucar.nc2.NetcdfFile
import ucar.nc2.NetcdfFiles
import ucar.nc2.Variable

import latis.data._
import latis.data.Data._
import latis.model._
import latis.ops.Head
import latis.ops.Operation
import latis.ops.Stride
import latis.util.LatisException
import latis.util.Section

/**
 * Helper class for reading data via the NetcdfFile API.
 */
protected class NetcdfWrapper private (ncFile: NetcdfFile, model: DataType, config: NetcdfAdapter.Config) {

  private lazy val domainScalars: List[Scalar] = model match {
    case Function(d, _) => d.getScalars
    case _ => List.empty
  }

  private lazy val rangeScalars: List[Scalar] = model match {
    case Function(_, r) => r.getScalars
    case _ => List.empty
  }

  /**
   * Returns the netCDF Dimension represented by the given Scalar.
   *
   * This uses the "sourceId" metadata property if it exists, otherwise
   * the scalar id. Note that validation ensures that it exists.
   */
  private def findDimension(scalar: Scalar): Dimension =
    ncFile.findDimension(scalar.ncName)

  /**
   * Returns the netCDF Variable represented by the given Scalar.
   *
   * This uses the "sourceId" metadata property if it exists, otherwise
   * the scalar id. Note that validation ensures that it exists.
   */
  private def findVariable(scalar: Scalar): Variable =
    ncFile.findVariable(scalar.ncName)

  /** Applies operations to define the subset of data to read. */
  private[input] def makeSection(ops: Seq[Operation]): IO[Section] = {
    domainScalars.traverse {
      case i: Index => IO.pure(findDimension(i).getLength)    //Index uses dimension name
      case s        => IO.pure(findVariable(s).getSize.toInt) //assume single dimension < max int
    }.flatMap { shape =>
      val init = Section.fromShape(shape) //original section before operations
      IO.fromEither(ops.foldM(init)(applyOperationToSection))
    }
  }

  /** Applies an operation to modify a Section. */
  private def applyOperationToSection(section: Section, op: Operation): Either[LatisException, Section] =
    op match {
      case _: Head        => section.head.asRight
      case Stride(stride) => section.stride(stride)
      case _              =>
        // Bug if we get here. NetcdfAdapter.canHandleOperation should not allow unsupported operation.
        LatisException(s"Unsupported operation: $op").asLeft
    }

  /** Returns a stream of samples for the given subset. */
  private[input] def streamSamples(section: Section): Stream[IO, Sample] =
    streamDomain(section).zip(streamRange(section))

  /** Returns a stream of domain data for the given subset. */
  private def streamDomain(section: Section): Stream[IO, DomainData] = domainScalars.length match {
    case 0 => Stream.raiseError[IO](LatisException("Zero-arity dataset not yet supported."))
    case 1 => streamDomain1D(section, domainScalars.head)
    case 2 => streamDomain2D(section, domainScalars.head, domainScalars(1))
    case _ => Stream.raiseError[IO](LatisException("NetcdfAdapter supports up to 2 dimensions only, for now."))
  }

  /** Returns a stream of domain data for a one-dimensional domain. */
  private def streamDomain1D(section: Section, s: Scalar): Stream[IO, DomainData] = s match {
    case _: Index => Stream(DomainData()).repeatN(section.length.get) //section should not be unlimited
    case _        => streamVariable(s, section).map(d => DomainData(d))
  }

  /**
   * Returns a stream of 2D domain data with the appropriate slice of the given
   * Section applied to each dimension.
   */
  private def streamDomain2D(section: Section, s1: Scalar, s2: Scalar): Stream[IO, DomainData] =
    //TODO: deal with Index, not yet supported for multi-dimensional datasets
    for {
      slice1 <- Stream.fromEither[IO](section.slice(0))
      slice2 <- Stream.fromEither[IO](section.slice(1))
      d1     <- streamVariable(s1, slice1)
      d2     <- streamVariable(s2, slice2) //TODO: memoize to avoid re-reading nested domain variables
    } yield DomainData(d1, d2)

  /**
   * Combines the streams of each range variable into a stream of RangeData.
   */
  private def streamRange(section: Section): Stream[IO, RangeData] = {
    val streams = rangeScalars.map(s => streamVariable(s, section))
    val start = streams.head.map(d => RangeData(d)) //first range variable to seed the fold
    streams.tail.foldLeft(start) { (s1, s2) =>
      s1.zipWith(s2)(_ :+ _) //RangeData is just List[Data] so we can append
    }
  }

  /**
   * Streams the data for a subset of the given variable.
   */
  private def streamVariable(scalar: Scalar, section: Section): Stream[IO, Datum] = {

    // Make a lean function to get data of the right type from a ucar.ma2.Array
    def makeValueGetter(arr: NcArray): Int => Datum = {
      scalar.valueType match {
        case BooleanValueType => (i: Int) => BooleanValue(arr.getBoolean(i))
        case ByteValueType    => (i: Int) => ByteValue(arr.getByte(i))
        case CharValueType    => (i: Int) => CharValue(arr.getChar(i))
        case ShortValueType   => (i: Int) => ShortValue(arr.getShort(i))
        case IntValueType     => (i: Int) => IntValue(arr.getInt(i))
        case LongValueType    => (i: Int) => LongValue(arr.getLong(i))
        case DoubleValueType  => (i: Int) => DoubleValue(arr.getDouble(i))
        case FloatValueType   => (i: Int) => FloatValue(arr.getFloat(i))
        case StringValueType  => (i: Int) => StringValue(arr.getObject(i).toString)
        case vt               =>
          throw LatisException(s"Unsupported data type: $vt") //bug, should be caught earlier
      }
    }

    // Read a section of the array for this variable
    val io = for {
      variable <- IO.pure(findVariable(scalar))
      section  <- IO(new NcSection(section.toString())) //may throw
      array    <- IO.blocking(variable.read(section))
    } yield {
      val getValue: Int => Datum = makeValueGetter(array) //function to get the ith value from the array
      (0 until array.getSize.toInt).map(getValue)
    }
    Stream.evalSeq(io)
  }

  /** Breaks the Section up into manageable contiguous Sections. */
  private[input] def chunkSection(section: Section): Stream[IO, Section] = {
    //TODO: consider better default chunk size
    // Note that the chunkSize property is optional rather than imposing a
    // default value up front. This gives us the flexibility to use netCDF
    // file chunking and caching properties to derive a better default.
    val size = config.chunkSize.getOrElse(4096)
    section.chunk(size)
  }

}


object NetcdfWrapper {

  /**
   * Creates a Resource with a NetcdfWrapper that encapsulates a NetcdfFile for the given URI.
   *
   * If the NetcdfFile is found to be inconsistent with the model, this will raise an error.
   * This uses the NetcdfValidator to ensure:
   *  - The model is a Function with no nested Functions
   *  - The NetcdfFile contains all the expected variables
   *  - The types of the variables are consistent
   *  - The dimensionality (shape) of each variable is consistent
   *
   * The Resource ensures that the NetcdfFile will be closed.
   */
  def open(
    uri: URI,
    model: DataType,
    config: NetcdfAdapter.Config = new NetcdfAdapter.Config()
  ): Resource[IO, NetcdfWrapper] = {
    val location: String = uri.getScheme match {
      case null   => uri.getPath //assume file path
      case "file" => uri.getPath
      case _      => uri.toString //TODO: fail proactively vs let netcdf throw?
    }

    //TODO: validate model in adapter before getting here, part of bigger Adapter redesign
    NetcdfValidator.validateModel(model) match {
      case Left(errs) =>
        val msg = errs.mkString(s"Invalid model for NetcdfAdapter: $model\n  ", "\n  ", "")
        Resource.eval(IO.raiseError(LatisException(msg)))
      case _ =>
        Resource.make(IO.blocking(NetcdfFiles.open(location))) { ncFile =>
          IO.blocking(ncFile.close())
        }.flatMap { ncFile =>
          NetcdfValidator.validateNetcdfFile(ncFile, model) match {
            case Left(errs) =>
              val msg = errs.mkString("Failed to make NetcdfWrapper:\n  ", "\n  ", "")
              Resource.eval(IO.raiseError(LatisException(msg)))
            case _ => Resource.pure(new NetcdfWrapper(ncFile, model, config))
          }
        }
    }
  }

}

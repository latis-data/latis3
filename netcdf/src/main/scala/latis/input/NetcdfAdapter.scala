package latis.input

import java.net.URI
import java.nio.file._

import cats.effect.IO
import fs2.Stream
import ucar.ma2.{Array => NcArray}
import ucar.ma2.{Range => URange}
import ucar.ma2.Section
import ucar.nc2.{Variable => NcVariable}
import ucar.nc2.dataset.NetcdfDataset

import latis.data._
import latis.model._
import latis.ops.Operation
import latis.ops.Stride
import latis.util._

/**
 * Defines an Adapter for NetCDF data sources.
 * This handles some operations by applying them to a
 * ucar.ma2.Section that us used when reading the data.
 */
case class NetcdfAdapter(
  model: DataType,
  config: NetcdfAdapter.Config = NetcdfAdapter.Config()
) extends Adapter {

  /**
   * Specifies which operations that this adapter will handle.
   */
  override def canHandleOperation(op: Operation): Boolean = op match {
    case Stride(stride) if (stride.length == model.arity) => true
    //TODO: domain selections, take, drop, ...
    case _ => false
  }

  /**
   * Reads the data for the modeled variables and section
   * as defined by the operations.
   * The data will be read into memory as a SetFunction.
   */
  def getData(uri: URI, ops: Seq[Operation]): SampledFunction = {
    val ncStream: Stream[IO, NetcdfDataset] = NetcdfAdapter.open(uri)
    ncStream.map { ncDataset =>
      val nc = NetcdfWrapper(ncDataset, model, config)

      // Applies ops to default section
      val section: Section = nc.applyOperations(ops)

      // Assumes all domain variables are 1D and define a Cartesian Product set.
      val domainSet: DomainSet = model match {
        case Function(domain, _) =>
          val dsets: List[DomainSet] = domain.getScalars.zipWithIndex.map {
            case (scalar, index) =>
              val sec   = new Section(section.getRange(index))
              nc.readVariable(scalar.id, sec).map { ncArr =>
                val ds: IndexedSeq[DomainData] =
                  (0 until ncArr.getSize.toInt).map { i =>
                    Data.fromValue(ncArr.getObject(i)) match {
                      case Right(d: Datum) => DomainData(d)
                      case Left(le) => throw le //TODO: error or drop?
                    }
                  }
                SeqSet1D(scalar, ds)
              }.getOrElse {
                // Variable not found, use index
                val r = section.getRange(index)
                IndexSet1D(r.first, r.stride, r.length)
              }
          }
          if (dsets.length == 1) dsets.head
          else ProductSet(dsets)
      }

      // Note, all range variables must have the same shape
      // consistent with the domain set
      val rangeData: IndexedSeq[RangeData] = model match {
        case Function(_, range) =>
          // Read the NcArray for each range variable
          val arrs: List[NcArray] = range.getScalars.flatMap { scalar =>
          //TODO: beware of silent failure if var not found
            nc.readVariable(scalar.id, section)
          }
          (0 until arrs.head.getSize.toInt).map { i =>
            RangeData(arrs.map { a =>
              Data.fromValue(a.getObject(i)) match {
                case Right(d) => d
                case Left(le) => throw le //TODO: error or fill?
              }
            })
          }
      }

      SetFunction(domainSet, rangeData)
    }.compile.toVector.unsafeRunSync().head //Note, will close file

  }

}

//TODO: move some of this to NetcdfUtils?
object NetcdfAdapter {

  /**
   * Defines a NetcdfAdapter specific configuration with type-safe accessors for
   * section.
   */
  case class Config(properties: (String, String)*) extends ConfigLike {
    //Note: ucar.ma2.Section is not serializable, use string representation instead
    val section: Option[String] = get("section")
  }

  /**
   * Returns a NetcdfDataset wrapped in an effectful Stream from the given URI.
   * The Stream provides resource management that will ensure that the source
   * gets closed.
   * NetcdfDataset vs NetcdfFile: metadata conventions applied to data values.
   *   applies scale_factor and add_offset
   *   applies valid_range and _FillValue resulting in NaN
   *   TODO: scaled data seems to be float
   *     does missing_value end up as NaN?
   */
  def open(uri: URI): Stream[IO, NetcdfDataset] = {
    val path: String = uri.getScheme match {
      case null =>
        uri.getPath //assume file path
      case "s3" =>
        // Create a local file name
        val (bucket, key) = AWSUtils.parseS3URI(uri)
        val dir = LatisConfig.get("file.cache.dir") match {
          case Some(dir) => dir
          case None      => Files.createTempDirectory("latis").toString
        }
        val file = Paths.get(dir, bucket, key).toFile
        // If the file does not exist, make a local copy
        //TODO: deal with concurrency
        if (!file.exists) AWSUtils.copyS3ObjectToFile(uri, file)
        file.toString
      case "file" =>
        uri.getPath
      case _ =>
        uri.getScheme + "://" + uri.getHost + "/" + uri.getPath
    }

    Stream.bracket(IO {
      NetcdfDataset.openDataset(path)
    })(nc => IO(nc.close()))
  }

  /**
   * Applies an Operation to a Section to create a new Section.
   */
  def applyOperation(section: Section, op: Operation): Section = op match {
    case Stride(stride) => applyStride(section, stride.toArray)
  }

  //Note, must include stride even for 1-length dimensions
  def applyStride(section: Section, stride: Array[Int]): Section = {
    import collection.JavaConverters._
    if (section.getRank != stride.length) {
      val msg = s"Invalid rank for stride: ${stride.mkString(",")}}"
      throw LatisException(msg)
    }

    val rs: Array[URange] = section.getRanges.asScala.zipWithIndex.toArray.map {
      case (r, i) => new URange(r.first, r.last, r.stride * stride(i))
    }

    new Section(rs: _*)
  }
}

/**
 * Defines a wrapper for a NetCDF dataset that provides convenient access
 * and applies operations by modifying the requested Section.
 */
case class NetcdfWrapper(ncDataset: NetcdfDataset, model: DataType, config: NetcdfAdapter.Config) {

  /**
   * Defines a Map of LaTiS variable id to the corresponding NetCDF Variable.
   * Note, this will access the file but not read data arrays.
   */
  private lazy val variableMap: Map[String, NcVariable] = {
    //TODO: fail faster by not making this lazy?
    val ids = model.getScalars.map(_.id)
    val pairs = ids.flatMap { id =>
      val vname = getNcVarName(id)
      ncDataset.findVariable(vname) match {
        case v: NcVariable => Some((id, v))
        case null => None
          //Note, domain variables not found will be replaced by index
          //TODO: what about range vars?
          //val msg = s"NetCDF variable not found: $vname"
          //throw LatisException(msg)
      }
    }
    pairs.toMap
  }

  /**
   * Gets the section as defined in the config or else
   * makes a section for the entire dataset.
   */
  def defaultSection: Section = config.section match {
    case Some(spec) => new Section(spec) //TODO: error handling
    case None       =>
      // Complete Section
      // Note, we can't use ":" since it becomes a null Range in the Section
      // Use first range variable
      //TODO: risky to assume first range variable is suitable
      val id = model match {
        //TODO: assumes only Functions
        case Function(_, r) => r.getScalars.head.id
      }
      val spec = variableMap(id).getShape.map { n =>
        s"0:${n-1}"
      }.mkString(",")
      new Section(spec)
  }

  /**
   * Applies the given operations to define the final section to read.
   */
  def applyOperations(ops: Seq[Operation]): Section =
    ops.foldLeft(defaultSection)(NetcdfAdapter.applyOperation)

  // Note, get is safe since the id comes from the model in the first place
  private def getNcVarName(id: String): String =
    model.findVariable(id).get.metadata.getProperty("origName").getOrElse(id)

  /**
   * Reads the section of the given variable into a NcArray.
   * This is where the actual IO is done.
   */
  def readVariable(id: String, section: Section): Option[NcArray] =
    variableMap.get(id).map(_.read(section))

  //def close(): Unit = ncDataset.close() //ncStream.compile.drain.unsafeRunSync()
}

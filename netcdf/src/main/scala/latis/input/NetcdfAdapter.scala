package latis.input

import java.net.URI

import scala.jdk.CollectionConverters._
import scala.math.BigDecimal
import scala.math.max
import scala.math.min

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import ucar.ma2.Section
import ucar.ma2.{Array => NcArray}
import ucar.ma2.{Range => URange}
import ucar.nc2.NetcdfFiles.makeValidPathName
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.{Variable => NcVariable}

import latis.data._
import latis.model._
import latis.ops.Operation
import latis.ops.Selection
import latis.ops.Stride
import latis.ops.parser.ast._
import latis.util._

/**
 * Defines an Adapter for NetCDF data sources.
 * This handles some operations by applying them to a
 * ucar.ma2.Section that is used when reading the data.
 *
 * Selection operation is supported for datasets with "cadence" and "start" metadata
 * and domain values that can be converted to double.
 */
case class NetcdfAdapter(
  model: DataType,
  config: NetcdfAdapter.Config = NetcdfAdapter.Config()
) extends Adapter {

  /**
   * Specifies which operations that this adapter will handle.
   */
  override def canHandleOperation(op: Operation): Boolean = op match {
    case Stride(stride) if stride.length == model.arity => true
    case s@Selection(v, _, _) => Identifier.fromString(v).exists(
      vId => model.getVariable(vId).exists(
        v => v("cadence").nonEmpty && v("start").nonEmpty)) &&
        (s.getSelectionOp match {
          case Right(Gt) | Right(Lt) | Right(GtEq) | Right(LtEq) | Right(Eq) | Right(EqEq) => true
          case _ => false
    })
    //TODO: take, drop, ...
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

      // Apply ops to sections.
      val sections = nc.applyOperations(ops).fold(throw _, identity)
      // Zip scalars with their corresponding section.
      val (domainSections, rangeSections) = model.getScalars
          .zip(sections)
          .splitAt(model.arity)

      // Assumes all domain variables are 1D and define a Cartesian Product set.
      val domainSet: DomainSet = {
        val dsets: List[DomainSet] = domainSections.map { case (scalar, sec) =>
          nc.readVariable(scalar.id, sec).map { ncArr =>
            val ds: IndexedSeq[DomainData] =
              (0 until ncArr.getSize.toInt).map { i =>
                Data.fromValue(ncArr.getObject(i)).fold(throw _, DomainData(_))
                //TODO: error or drop?
              }
            SeqSet1D(scalar, ds)
          }.getOrElse {
            // Variable not found, use index
            // Throws NullPointerException if section contains ":"
            IndexSet1D(0, 1, sec.computeSize.toInt)
          }
        }
        if (dsets.length == 1) dsets.head
        else ProductSet(dsets)
      }

      // Note, all range variables must have the same shape
      // consistent with the domain set
      val rangeData: IndexedSeq[RangeData] = {
        val arrs: List[NcArray] = rangeSections.flatMap {
          case (scalar, section) =>
            //TODO: beware of silent failure if var not found
            nc.readVariable(scalar.id, section)
        }
        (0 until arrs.head.getSize.toInt).map { i =>
          RangeData(arrs.map { ncArr =>
            Data.fromValue(ncArr.getObject(i)).fold(throw _, identity)
            //TODO: error or fill?
          })
        }
      }

      SetFunction(domainSet, rangeData)
    }.compile.toVector.unsafeRunSync().head //Note, will close file

  }

}

//TODO: move some of this to NetcdfUtils?
object NetcdfAdapter extends AdapterFactory {
  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): NetcdfAdapter =
    new NetcdfAdapter(model, NetcdfAdapter.Config(config.properties: _*))

  /**
   * Defines a NetcdfAdapter specific configuration with type-safe accessors for
   * section.
   */
  case class Config(properties: (String, String)*) extends ConfigLike {
    //Note: ucar.ma2.Section is not serializable, use string representation instead
    val section: Option[String] = get("section")
      .map(_.replaceAll("""\s*""",""))  // trim whitespace
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
//      case "s3" =>
//        // Create a local file name
//        val (bucket, key) = AWSUtils.parseS3URI(uri)
//        val dir = LatisConfig.get("file.cache.dir") match {
//          case Some(dir) => dir
//          case None      => Files.createTempDirectory("latis").toString
//        }
//        val file = Paths.get(dir, bucket, key).toFile
//        // If the file does not exist, make a local copy
//        //TODO: deal with concurrency
//        if (!file.exists) AWSUtils.copyS3ObjectToFile(uri, file)
//        file.toString
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
  def applyOperation(
    sections: List[Section],
    model:DataType,
    op: Operation
  ): Either[LatisException, List[Section]] = op match {
    // TODO: revisit logic in each operation for multiple sections
    case Stride(stride) => NetcdfAdapter.applyStride(sections, model, stride.toArray)
    case sel: Selection => NetcdfAdapter.applySelection(sections, model, sel)
  }

  /**
   * Applies the given stride to the given sections. The length of the stride
   * array must match the arity of the given model. In the domain, the nth
   * stride is applied to the first ucar.ma2.Range of the nth Section. In the
   * range, the nth stride is applied to the nth ucar.ma2.Range of every Section.
   */
  def applyStride(
    sections: List[Section],
    model: DataType,
    stride: Array[Int]
  ): Either[LatisException, List[Section]] = {
    def applyStrideToRange(r: URange, s: Int): URange =
      new URange(r.first, r.last, r.stride * s)
    if (model.arity != stride.length) {
      Left(LatisException(s"Invalid rank for stride: ${stride.mkString(",")}"))
    } else {
      val domainSec =
        Either.catchOnly[NullPointerException] {
          sections.zip(stride).map {
            case (sec, str) =>
              val rs = sec.getRanges.asScala.toList
              val firstR = applyStrideToRange(rs.head, str)
              new Section(firstR :: rs.tail: _*)
          }
        }.leftMap(_ => LatisException("Can't apply a stride to a null Range."))

      val rangeSec =
        Either.catchOnly[NullPointerException] {
          sections.drop(stride.length).map { sec =>
            if (sec.getRank != stride.length) {
              throw LatisException(s"Invalid rank for stride: ${stride.mkString(",")}")
            } else {
              val rs = sec.getRanges.asScala.toList.zip(stride).map {
                case (r, str) => applyStrideToRange(r, str)
              }
              new Section(rs: _*)
            }
          }
        }.leftMap(_ => LatisException("Can't apply a stride to a null Range."))

      (domainSec, rangeSec).mapN(_ ++ _)
    }
  }

  /**
   * Applies the given selection operation to the given sections and returns new
   * sections. The first non-singleton ucar.ma2.Range in the section of the
   * selected variable has the selection applied. Then, all sections in the
   * model's range are searched for a ucar.ma2.Range that matches the one
   * replaced in the domain, and is replaced likewise.
   *
   * Indices for the range in the returned section are extrapolated from
   * "cadence" and "start" metadata without touching the actual data. If the
   * selected variable is a Time scalar of formatted strings, the "cadence" and
   * "start" metadata are assumed to be in units of milliseconds since
   * 1970-01-01T00:00:00.000Z.
   * @param sections sections of the adapter
   * @param model model containing a scalar with cadence and start metadata
   * @param selection selection operation to be applied
   * @return a new section list
   */
  def applySelection(
    sections: List[Section],
    model: DataType,
    selection: Selection
  ): Either[LatisException, List[Section]] = {

    /**
     * Creates a new range to replace the range in the given selection. The
     * index passed might not be an integer, but must be cast to an integer to
     * create a new range. It is checked if index is close to an int with things
     * like: index == index.toInt.toDouble
     */
    def getNewRange(range: URange, index: Double): Either[LatisException, URange] =
      selection.getSelectionOp match {
        case Left(e) => Left(e)
        case Right(Gt)   =>
          val indexCeil = index.ceil.toInt
          val adjustedLow = if (index == indexCeil.toDouble) indexCeil + 1
          else indexCeil
          val lowerRange = max(range.first, adjustedLow)
          if (lowerRange > range.last) Right(URange.EMPTY)
          else Right(new URange(lowerRange, range.last))
        case Right(Lt)   =>
          val indexFloor = index.floor.toInt
          val adjustedHigh = if (index == indexFloor.toDouble) indexFloor - 1
          else indexFloor
          val upperRange = min(range.last, adjustedHigh)
          if (range.first > upperRange) Right(URange.EMPTY)
          else Right(new URange(range.first, upperRange))
        case Right(GtEq) =>
          val lowerRange = max(index.ceil.toInt, range.first)
          if (lowerRange > range.last) Right(URange.EMPTY)
          else Right(new URange(lowerRange, range.last))
        case Right(LtEq) =>
          val upperRange = min(index.floor.toInt, range.last)
          if (range.first > upperRange) Right(URange.EMPTY)
          else Right(new URange(range.first, upperRange))
        case Right(Eq) | Right(EqEq) =>
          val intIndex = index.toInt
          if (index == intIndex.toDouble &&
            intIndex <= range.last &&
            intIndex >= range.first)
            Right(new URange(intIndex, intIndex))
          else Right(URange.EMPTY)
        case Right(op) => Left(LatisException(s"Unsupported selection operator $op"))
      }

    /** Gets a scalar's metadata and converts it to a double. */
    def getMetadataAsDouble(s: Scalar, key: String): Either[LatisException, Double] =
      s(key) match {
        case Some(v) => Either.catchOnly[NumberFormatException](v.toDouble)
          .leftMap(_ => LatisException(s"$v could not be converted to a double"))
        case _ => Left(LatisException(s"scalar $s does not have $key metadata"))
      }

    /** Returns value as a BigDecimal. If value is a formatted string, return ms since 1970. */
    def getBigDecimalValue(s: Scalar, value: String): Either[LatisException, BigDecimal] = {
      s match {
        case scalar: latis.time.Time if scalar.isFormatted =>
          scalar.timeFormat.get.parse(value).map(BigDecimal(_))
        case _ => Either.catchOnly[NumberFormatException](BigDecimal(value))
      }
    }.leftMap(_ => LatisException(s"$value could not be converted to a BigDecimal"))

    /**
     * Gets the index where the select value would be given the starting value and
     * cadence of a dataset. The index is returned as a double since it is not
     * guaranteed to be an integer.
     */
    def getIndex(
      selectValue: BigDecimal,
      firstValue: Double,
      cadence: Double
    ): Either[LatisException, Double] =
      Right(((selectValue - firstValue) / cadence).toDouble)

    /** Gets the zero-indexed position of a domain scalar in a top-level function. */
    def getScalarPos(id: String): Either[LatisException, Int] =
      model.getPath(id) match {
        case None => Left(LatisException(s"$id not found in model."))
        case Some(List(DomainPosition(n))) => Right(n)
        case _ => Left(LatisException(s"$id must be in the domain of a " +
          "top-level function in the model."))
      }

    /** Finds and returns the first Range with length > 1 in given Section. */
    def getOldRange(s: Section): Either[LatisException, URange] =
      s.getRanges.asScala.toList.find(_.length > 1)
        .toRight(LatisException(s"Could not find viable range to apply selection in $s."))

    /** Finds first occurrence of oldRange and replaces with newRange in given Section. */
    def replaceRangeInSection(
      oldRange: URange,
      newRange: URange,
      sec: Section
    ): Either[LatisException, Section] = {
      val ranges = sec.getRanges.asScala.toList
      ranges.zipWithIndex.find { case (r, _) => r == oldRange} match {
        case Some((_, n)) =>
          val newRanges = ranges.take(n) ++ List(newRange) ++ ranges.drop(n + 1)
          Right(new Section(newRanges: _*))
        case _ => Left(LatisException("Could not find viable range to apply selection."))
      }
    }

    for {
      scalar <- selection.getScalar(model)
      cadence <- getMetadataAsDouble(scalar, "cadence")
      firstValue <- getMetadataAsDouble(scalar, "start")
      selectValue <- getBigDecimalValue(scalar, selection.value)
      index <- getIndex(selectValue, firstValue, cadence)
      pos <- getScalarPos(scalar.id)
      // find the URange to apply the selection to
      oldRange <- getOldRange(sections(pos))
      // apply the selection to get a new URange
      newRange <- getNewRange(oldRange, index)
      // update the URange in the Section it came from
      domainSection <- replaceRangeInSection(oldRange, newRange, sections(pos))
      domainSections = sections.take(pos) ++ List(domainSection) ++
        sections.slice(pos + 1, model.arity)
      // update the appropriate URange in every Section in the range
      rangeSections <- sections.drop(model.arity)
        .traverse(replaceRangeInSection(oldRange, newRange, _))
    } yield domainSections ++ rangeSections

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
    def getNcVar(id: String): Option[NcVariable] = {
      val validId = makeValidPathName(
        model.findVariable(id)
        .flatMap(_("sourceId"))
        .getOrElse(id)
      )
      Option(ncDataset.findVariable(validId))
    }

    //TODO: fail faster by not making this lazy?
    val ids = model.getScalars.map(_.id)
    val pairs = ids.flatMap { id =>
      getNcVar(id).map((id, _))
      //Note, domain variables not found will be replaced by index
      //TODO: what about range vars?
      //val msg = s"NetCDF variable not found: $vname"
      //throw LatisException(msg)
    }
    pairs.toMap
  }

  private def idToSectionString(id: String): String = variableMap.get(id) match {
    case Some(v) => v.getShape.map(n => s"0:${n - 1}").mkString(",")
    case _ => ""
  }

  private def makeSectionFromIdAndString(id: String, sec: String): Section = {
    val rangeStrings = sec.split(',')
    if (rangeStrings.contains(":")) {
      val secString = idToSectionString(id).split(',').zip(rangeStrings).map {
        case (r, ":") => r
        case (_, r) => r
      }.mkString(",")
      new Section(secString)
    } else {
      new Section(sec)
    }
  }

  /**
   * Gets the sections as defined in the config or else makes a section for each
   * variable in the dataset. If only one section is specified in the config,
   * then the number of ranges in the section is checked to match the number of
   * scalars in the model, and the section is then split into a list of sections.
   */
  lazy val sections: List[Section] = {
    val ids = model.getScalars.map(_.id)
    config.section match {
      case Some(spec) =>
        val sectionsNotNull = ids.zip(spec.split(';')).map {
          case (id, str) => makeSectionFromIdAndString(id, str)
        }
        sectionsNotNull match {
          //TODO: error handling. Throws ucar.ma2.InvalidRangeException and
          // java.lang.IllegalArgumentException
          case sec :: Nil =>  // Only one section.
            // Each domain variable uses one ma2.Range and the range variables
            // use the entire section each.
            val ranges = sec.getRanges.asScala.toList
            if (ranges.length == model.arity)
              ranges.map(new Section(_)) ++ List.fill(ids.length - ranges.length)(sec)
            else throw LatisException("The number of ranges in the adapter section " +
              "must match the number of scalars in the domain of the model.")
          case sections =>
            // if there are many sections, then there should be one for each variable
            if (sections.length == ids.length) sections
            else throw LatisException("The number of sections defined in the adapter " +
              "must be either 1 or match the number of scalars in the model.")
        }
      case None =>
        // Makes a section for each variable in the model. Gets the shape of
        // each variable to select the full range of every dimension. If no
        // variable is found, use an empty section (no ranges).
        ids.map(id => new Section(idToSectionString(id)))
    }
  }

  /**
   * Applies the given operations to define the final section to read.
   */
  def applyOperations(ops: Seq[Operation]): Either[LatisException, List[Section]] =
    ops.toList.foldM(sections)(NetcdfAdapter.applyOperation(_, model, _))

  /**
   * Reads the section of the given variable into a NcArray.
   * This is where the actual IO is done.
   */
  def readVariable(id: String, section: Section): Option[NcArray] =
    variableMap.get(id).map(_.read(section))

  //def close(): Unit = ncDataset.close() //ncStream.compile.drain.unsafeRunSync()
}

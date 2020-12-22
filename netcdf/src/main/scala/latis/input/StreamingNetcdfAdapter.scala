package latis.input

import java.net.URI

import scala.jdk.CollectionConverters._

import cats.effect.IO
import fs2.Stream
import ucar.ma2.{Array => NcArray}
import ucar.ma2.{Range => URange}
import ucar.ma2.Section

import latis.data._
import latis.model._
import latis.ops.Operation
import latis.ops.Selection
import latis.ops.Stride
import latis.util._
import latis.util.dap2.parser.ast._

/**
 * Experimental copy of NetcdfAdapter that reads and streams
 * chunks of data from the netCDF file so we don't have to
 * read it all into memory.
 */
case class StreamingNetcdfAdapter(
  model: DataType,
  config: NetcdfAdapter.Config = NetcdfAdapter.Config()
) extends Adapter {

  /**
   * Specifies which operations that this adapter will handle.
   */
  override def canHandleOperation(op: Operation): Boolean = op match {
    case Stride(stride) if stride.length == model.arity => true
    case s@Selection(v, _, _) => model.getVariable(v).exists(
      v => v("cadence").nonEmpty && v("start").nonEmpty) &&
      (s.operator match {
        case Gt | Lt | GtEq | LtEq | Eq | EqEq | Tilde => true
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
    val stream: Stream[IO, Sample] = NetcdfAdapter.open(uri).flatMap { ncDataset =>
      val nc = NetcdfWrapper(ncDataset, model, config)
      // Apply ops to sections.
      val sections = nc.applyOperations(ops).fold(throw _, identity)
      // Zip scalars with their corresponding section.
      val (domainSections, rangeSections) = model.getScalars
        .zip(sections)
        .splitAt(model.arity)

      // Creates 1D sets for domain variables
      // TODO: avoid reading first coordinate variable since it is the dimension
      //  we want to chunk on. We only need the length of this dimension
      //  but we do want the sectioning from the operations to be applied
      //  so we need to be more clever about the index used for the chunks below.
      //  This is still useful for datasets with rank > 1.
      val dsets: List[DomainSet] = domainSections.map { case (scalar, sec) =>
        val scalarId = scalar.id.getOrElse(throw LatisException("Scalar must have an identifier"))
        nc.readVariable(scalarId, sec).map { ncArr =>
          val ds: IndexedSeq[DomainData] =
            (0 until ncArr.getSize.toInt).map { i =>
              Data.fromValue(ncArr.getObject(i)).fold(throw _, DomainData(_))
              //TODO: error or drop?
            }
          SeqSet1D(scalar, ds)
        }.getOrElse {
          // Variable not found, use index
          IndexSet1D(0, 1, sec.computeSize.toInt)
        }
      }

      // Defines Chunks of Samples by breaking up the first dimension.
      // Start with chunk size = 1 element of the first dimension.
      //   This will likely be slow for datasets with large first dimension.
      // TODO: look into using chunks as defined in file
      Stream.emits(0 until dsets.head.length).flatMap { index =>
        // Define the domain data for a chunk
        val set1: DomainSet = dsets.head(index).map { dd =>
          SeqSet1D(dsets.head.model, Vector(dd))
        }.getOrElse{
          throw LatisException(s"Invalid index: $index") //TODO: raise into Stream
        }
        val domainData: Seq[DomainData] = ProductSet(set1 +: dsets.tail).elements //TODO: does ProductSet of one work?

        // Make a new Section with a single element of the first dimension.
        // Assume that all variables in the range have the same Section.
        //   this won't work with section definitions in the fdml
        val sectionRanges: List[URange] = rangeSections.head._2.getRanges.asScala.toList
        val newRanges = (new URange(index, index) +: sectionRanges.tail).asJava
        val section = new Section(newRanges)

        // Read sections of range data
        val rangeData: IndexedSeq[RangeData] = {
          // Read NcArray for each range variable
          val arrs: List[NcArray] = rangeSections.flatMap {
            case (scalar, _) =>
              //TODO: beware of silent failure if var not found
              val scalarId = scalar.id.getOrElse(throw LatisException("Scalar must have an identifier"))
              nc.readVariable(scalarId, section)
          }
          // Assume all have same shape. Required but not verified.
          (0 until arrs.head.getSize.toInt).map { i =>
            RangeData(arrs.map { ncArr =>
              Data.fromValue(ncArr.getObject(i)).fold(throw _, identity)
              //TODO: error or fill?
            })
          }
        }

        Stream.emits(domainData.zip(rangeData).map(t => Sample(t._1, t._2)))
      }
    }
    StreamFunction(stream)
  }

}

object StreamingNetcdfAdapter extends AdapterFactory {
  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): StreamingNetcdfAdapter =
    new StreamingNetcdfAdapter(model, NetcdfAdapter.Config(config.properties: _*))
}

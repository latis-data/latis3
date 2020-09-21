package latis.data

import latis.util.DefaultDomainOrdering
import latis.util.LatisException

/**
 * Defines a SampledFunction implemented with a 2D array.
 * The domain values are 0-based indices as Ints.
 */
case class ArrayFunction2D(array: Array[Array[RangeData]]) extends MemoizedFunction {

  /**
   * Defines the ordering for the domain indices.
   */
  def ordering: Option[PartialOrdering[DomainData]] = Some(DefaultDomainOrdering)

  override def eval(data: DomainData): Either[LatisException, RangeData] = data match {
    case RangeData(Index(i), Index(j)) =>
      array.lift(i).flatMap(_.lift(j)).toRight {
        val msg = s"No sample found matching $data"
        LatisException(msg)
      }
    case _ => Left(LatisException(s"Invalid evaluation value for ArrayFunction2D: $data"))
  }

  def sampleSeq: Seq[Sample] =
    Seq
      .tabulate(array.length, array(0).length) { (i, j) =>
        Sample(DomainData(i, j), RangeData(array(i)(j)))
      }
      .flatten

}

object ArrayFunction2D extends FunctionFactory {
  // Assumes the samples are a function of index with no gaps
  // so we can determine the shape of the array from the domain
  // of the last sample
  def fromSamples(samples: Seq[Sample]): MemoizedFunction = samples match {
    case Seq() => ??? // TODO: figure out how to handle error, use Either
    case _ =>
      samples.last.domain match {
        case DomainData(Index(x), Index(y)) =>
          val nx    = x + 1
          val ny    = y + 1
          val array = Array.tabulate(nx, ny)((i, j) => RangeData(samples(i * ny + j).range))
          ArrayFunction2D(array)
        case _ => ??? //TODO: error, invalid sample type
      }
  }

  def fromData(array: Array[Array[Data]]): ArrayFunction2D =
    //TODO: ensure that each nested array has the same length (Cartesian)
    //  return Either
    ArrayFunction2D(array.map(_.map(RangeData(_))))
}

package latis.data

import latis.util.LatisException

/**
 * A SampledFunction implemented with a 2D array.
 * The domain values are 0-based indices as Ints.
 */
case class ArrayFunction2D(array: Array[Array[TupleData]]) extends MemoizedFunction {

  override def apply(value: TupleData): Either[LatisException, TupleData] = value.elements match {
    case List(Index(i), Index(j)) => array.lift(i).flatMap(_.lift(j)) match {
      case Some(r) => Right(r)
      case None =>
        val msg = s"No sample found matching $value"
        Left(LatisException(msg))
    }
    case _ => Left(LatisException(s"Invalid evaluation value for ArrayFunction1D: $value"))
  }

  def sampleSeq: Seq[Sample] =
    Seq.tabulate(array.length, array(0).length) { (i, j) =>
      Sample(DomainData(i, j), RangeData(array(i)(j).elements))
    }.flatten

}

object ArrayFunction2D extends FunctionFactory {
  // Assumes the samples are a function of index with no gaps
  // so we can determine the shape of the array from the domain
  // of the last sample
  def fromSamples(samples: Seq[Sample]): MemoizedFunction = samples match {
    case Seq() => ??? // TODO: figure out how to handle error
    case _ =>
      samples.last.domain match {
        case DomainData(Index(x), Index(y)) =>
          val nx = x + 1
          val ny = y + 1
          val array = Array.tabulate(nx, ny)((i, j) => TupleData(samples(i * ny + j).range))
          ArrayFunction2D(array)
        case _ => ??? //TODO: error, invalid sample type
      }
  }

}

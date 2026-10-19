package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.LatisException

case class ExcludeMissing() extends StreamOperation {
  def pipe(model: DataType): Pipe[IO, Sample, Sample] = {
    def predicate(model: DataType, rangedata: RangeData): Option[List[Data]] = {
    
      def go(dt: DataType, range: RangeData): Option[List[Data]] = {
        dt match {
          case s: Scalar =>
            if (range.length == 1) {
              if (s.isMissing(range(0))) {
                None
              } else {
                Some(range)
              }
            } else {
              throw LatisException("Expected range of Sample for a Scalar to hold one value")
            }
          case t: Tuple =>
            val tupleElements = t.elements

            if (tupleElements.length == range.length) {
              val newList: Option[List[List[Data]]] = tupleElements.zip(range).map { (dt, ds) =>
                go(dt, RangeData(ds))
              }.sequence

              newList.map(_.flatten)
            } else {
              throw LatisException("Expected range of Sample for a Tuple to hold same number of elements")
            }
          case Function(_, rn) =>
            range match {
              case List(MemoizedFunction(sampleSeq)) =>
                val newSampleSeq: Seq[Sample] = sampleSeq.collect { sample =>
                  go(rn, sample.range) match {
                    case Some(newRange) => Sample(sample.domain, newRange)
                  }
                }
                Some(List(SampledFunction(newSampleSeq)))
              case _ => go(rn, range)
            }
        }
      }

      go(model, rangedata)
    }

    (stream: Stream[IO, Sample]) => {
      stream.collect { sample => 
        predicate(model, sample.range) match {
          case Some(newRange) => Sample(sample.domain, newRange)
        }
      }
    }
  }

  // model is unchanged
  override def applyToModel(model: DataType): Either[LatisException, DataType] = 
    model.asRight
}

object ExcludeMissing {
  def builder: OperationBuilder = (args: List[String]) =>
    Either.cond(
      args.isEmpty,
      ExcludeMissing(),
      LatisException("ExcludeMissing takes no arguments")
    )
}

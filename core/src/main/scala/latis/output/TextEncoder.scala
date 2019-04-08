package latis.output

import scala.util.Properties.lineSeparator

import cats.effect.IO
import fs2.Stream

import latis.data._
import latis.model._

object TextEncoder extends Encoder[IO, String] {

  /**
   * Encode the Stream of Samples from the given Dataset as a Stream
   * of Strings.
   */
  override def encode(dataset: Dataset): Stream[IO, String] = {
    // Create a Stream with the header
    val header: Stream[IO, String] = Stream.emit(dataset.toString)

    // Encode each Sample as a String in the Stream
    val samples: Stream[IO, String] =
      dataset.data.streamSamples.flatMap(encodeSample(dataset.model, _))

    // Combine the output into a single Stream
    header ++ samples
  }

  /**
   * Given a Sample and its data model, create a Stream of Strings.
   */
  def encodeSample(model: DataType, sample: Sample): Stream[IO, String] = {
    (model, sample) match {
      case (Function(domain, range), Sample(ds, rs)) =>
        (encodeData(domain, ds) ++ encodeData(range, rs))
          .chunkN(2).map(_.toVector.mkString(" -> "))
    }
  }

  def encodeData(model: DataType, data: Seq[_]): Stream[IO, String] = {
    val ds = scala.collection.mutable.Stack(data: _*)

    def go(dt: DataType): Stream[IO, String] = dt match {
      //TODO: error if ds is empty
      case _: Scalar => Stream.emit(ds.pop.toString)  //TODO: delegate to dt to produce String?

      case Tuple(es @ _*) =>
        Stream.emits(es)
          .flatMap(go(_))
          .chunkN(es.length)
          .map(_.toVector.mkString("(", ",", ")"))

      case f: Function => ds.pop match {
        case sf: SampledFunction => encodeFunction(f, sf)
        case _ => ??? //Oops, model and data not consistent
      }
    }

    go(model)
  }

  //nested function
  //TODO: has problems, see hysics after groupBy
  //TODO: indent
  def encodeFunction(ftype: Function, function: SampledFunction): Stream[IO, String] = {
    val head: Stream[IO, String] = Stream.emit(s"{$lineSeparator")

    val delim = lineSeparator //TODO: FirstThenOther?
                        //TODO: chunkN map to Vector then mkString? need to know length,
                        //  OK since this is a nested Function
                        //  can we tell Stream to chunk all?
    val samples: Stream[IO, String] =
      function.streamSamples.flatMap(encodeSample(ftype, _)).map(_ + delim)

    val foot: Stream[IO, String] = Stream.emit("}")

    head ++ samples ++ foot
  }
}

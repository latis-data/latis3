package latis.output

import scala.util.Properties.lineSeparator

import cats.effect.IO
import fs2.Stream

import latis.data._
import latis.model._
import latis.util.StreamUtils

object TextEncoder extends Encoder[IO, String] {
  
  /**
   * Track the level of Function nesting so we can indent.
   */
  //TODO: handle via recursion, doesn't work with lazy stream
  private var functionIndent: Int = 0

  /**
   * Encode the Stream of Samples from the given Dataset as a Stream
   * of Strings.
   */
  override def encode(dataset: Dataset): Stream[IO, String] = {
    // Create a Stream with the header
    val header: Stream[IO, String] = Stream.emit(dataset.toString + lineSeparator)

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
        (encodeData(domain, ds) zip encodeData(range, rs)) map { p =>
          " " * functionIndent + s"${p._1} -> ${p._2}$lineSeparator"
        }
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

      // Nested Function
      case f: Function => ds.pop match {
        case sf: SampledFunction => encodeFunction(f, sf)
        case _ => ??? //Oops, model and data not consistent
      }
    }

    go(model)
  }

  // Nested function
  def encodeFunction(ftype: Function, function: SampledFunction): Stream[IO, String] = {
    val head: Stream[IO, String] = Stream.emit(s"{$lineSeparator")
    functionIndent += 2

    val samples: Stream[IO, String] =
      function.streamSamples.flatMap(encodeSample(ftype, _))
    
    functionIndent -= 2
    val foot: Stream[IO, String] = Stream.emit("}")

    // Combine nested function into a stingle string
    (head ++ samples ++ foot).fold1((a: String, b: String) => a + b)
  }
}

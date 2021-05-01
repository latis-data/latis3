package latis.output

import scala.util.Properties.lineSeparator

import cats.effect.IO
import fs2.Stream

import latis.data._
import latis.dataset.Dataset
import latis.model._
import latis.util.Identifier

class TextEncoder extends Encoder[IO, String] {

  /**
   * Tracks the level of Function nesting so we can indent.
   */
  //TODO: handle via recursion, doesn't work with lazy stream
  private var functionIndent: Int = 0

  /**
   * Encodes the Stream of Samples from the given Dataset as a Stream
   * of Strings.
   */
  override def encode(dataset: Dataset): Stream[IO, String] = {
    // Create a Stream with the header
    val header: Stream[IO, String] =
      Stream.emit(dataset.model.toString + lineSeparator)

    // Encode each Sample as a String in the Stream
    val samples: Stream[IO, String] =
      dataset.samples.map(encodeSample(dataset.model, _) + lineSeparator)

    // Combine the output into a single Stream
    header ++ samples
  }

  /**
   * Given a Sample and its data model, creates a Stream of Strings.
   */
  def encodeSample(model: DataType, sample: Sample): String =
    (model, sample) match {
      case (Function(domain, range), Sample(ds, rs)) =>
        " " * functionIndent +
          s"${encodeData(domain, ds)} -> ${encodeData(range, rs)}"
      case (_, Sample(_, rs)) =>
        encodeData(model, rs)
    }

  def encodeData(model: DataType, data: Seq[Data]): String = {
    val ds = scala.collection.mutable.Stack(data: _*)

    def go(dt: DataType): String = dt match {
      //TODO: error if ds is empty

      case i: latis.model.Index =>
        // No data in Sample, generate index
        IndexGenerator.nextIndex(i.id.get).toString

      case s: Scalar =>
        ds.pop() match {
          case d: Datum => s.formatValue(d)
          case _ => ??? //bug, inconsistent data
        }

      case Tuple(es @ _*) =>
        es.map(go).mkString("(", ", ", ")")

      // Nested Function
      case f: Function =>
        ds.pop() match {
          case sf: MemoizedFunction => encodeFunction(f, sf)
          case _ => ??? //Oops, model and data not consistent
        }
    }

    go(model)
  }

  // Nested function
  def encodeFunction(ftype: Function, function: MemoizedFunction): String = {
    val head: String = "{" + lineSeparator
    functionIndent += 2

    val samples: Seq[String] = function.sampleSeq.map(encodeSample(ftype, _))

    functionIndent -= 2
    val foot: String = lineSeparator + "}"

    // Combine nested function into a single string
    head ++ samples.mkString(lineSeparator) ++ foot
  }
}

/**
 * Provides incrementing index values based on id.
 */
private object IndexGenerator {
  val indexMap = scala.collection.mutable.Map[Identifier, Long]()
  def nextIndex(id: Identifier): Long = {
    val index = indexMap.getOrElse(id, 0L)
    indexMap += ((id, index + 1))  //add or replace
    index
  }
}

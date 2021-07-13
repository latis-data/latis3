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
   *  Supports incrementing index values.
   *  This will be replaced with a fresh generator with each call to
   *  encode(dataset).
   */
  //TODO: use cats.effect.Ref
  private var indexGenerator: IndexGenerator = new IndexGenerator()

  /**
   * Encodes the Stream of Samples from the given Dataset as a Stream
   * of Strings.
   */
  override def encode(dataset: Dataset): Stream[IO, String] = {
    // Makes a fresh index generator
    indexGenerator = new IndexGenerator()

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
  private[output] def encodeSample(model: DataType, sample: Sample): String =
    model match {
      case Function(domain, range) =>
        " " * functionIndent +
          s"${encodeData(domain, sample.domain)} -> ${encodeData(range, sample.range)}"
      case _ =>
        encodeData(model, sample.range)
    }

  private[output] def encodeData(model: DataType, data: Seq[Data]): String = {
    val ds = scala.collection.mutable.Stack(data: _*)

    def go(dt: DataType): String = dt match {
      //TODO: not exhaustive: See https://github.com/latis-data/latis3/issues/304
      //TODO: error if ds is empty

      case i: latis.model.Index =>
        // No data in Sample, generate index
        indexGenerator.nextIndex(i.id).toString

      case s: Scalar =>
        ds.pop() match {
          case d: Data => s.formatValue(d)
          case _ => ??? //bug, inconsistent data
        }

      case t: Tuple =>
        t.elements.map(go).mkString("(", ", ", ")")

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
  private[output] def encodeFunction(ftype: Function, function: MemoizedFunction): String = {
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
private class IndexGenerator {
  val indexMap = scala.collection.mutable.Map[Identifier, Long]()
  def nextIndex(id: Identifier): Long = {
    val index = indexMap.getOrElse(id, 0L)
    indexMap += ((id, index + 1))  //add or replace
    index
  }
}

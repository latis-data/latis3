package latis.output

import scala.util.Properties.lineSeparator

import cats.effect.IO
import fs2.Stream

import latis.data.Datum
import latis.data.Sample
import latis.dataset.Dataset
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.ops.Uncurry

class CsvEncoder(header: Dataset => Stream[IO, String]) extends Encoder[IO, String] {

  /**
   * Encodes the Stream of Samples from the given Dataset as a Stream
   * of Strings with comma separated values.
   * @param dataset dataset to encode
   */
  override def encode(dataset: Dataset): Stream[IO, String] = {
    val uncurriedDataset = dataset.withOperation(Uncurry())
    // Encode each Sample as a String in the Stream
    header(dataset) ++ uncurriedDataset.samples
      .map(encodeSample(uncurriedDataset.model, _) + lineSeparator)
  }

  /**
   * Encodes a single Sample to a String of comma separated values.
   */
  def encodeSample(model: DataType, sample: Sample): String =
    (model, sample) match {
      case (Function(domain, range), Sample(ds, rs)) =>
        val scalars = domain.getScalars ++ range.getScalars
        val datas   = ds ++ rs
        scalars
          .zip(datas)
          .map {
            case (s: Scalar, d: Datum) =>
              s.formatValue(d)
            case _ => ???
          }
          .mkString(",")
    }
}

object CsvEncoder {

  /** Default encoder with no header */
  def apply(): CsvEncoder = new CsvEncoder(_ => Stream())

  def apply(header: Dataset => Stream[IO, String]): CsvEncoder = new CsvEncoder(header)

//  def apply(header: Dataset => String): CsvEncoder = new CsvEncoder(ds => Stream(header(ds)))

  def withColumnID: CsvEncoder = {
    def header(dataset: Dataset): Stream[IO, String] = dataset.model match {
      case Function(domain, range) =>
        Stream((domain.getScalars ++ range.getScalars).map(_.id).mkString(", "))
    }
    CsvEncoder(header)
  }
}

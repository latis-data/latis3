package latis.output

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import fs2.data.csv
import fs2.data.csv.Row
import fs2.data.csv.RowEncoder

import latis.data.Sample
import latis.dataset.Dataset
import latis.model.Index
import latis.model.Scalar
import latis.ops.Uncurry
import latis.util.LatisException

class CsvEncoder(header: Dataset => Stream[IO, String]) extends Encoder[IO, String] {

  /**
   * Encodes the Stream of Samples from the given Dataset as a Stream
   * of Strings with comma separated values.
   * @param dataset dataset to encode
   */
  override def encode(dataset: Dataset): Stream[IO, String] = {
    val uncurriedDataset = dataset.withOperation(Uncurry())

    // CSV encoding ignores Index scalars
    val scalars = NonEmptyList.fromList(
      uncurriedDataset
        .model
        .getScalars
        .filterNot(_.isInstanceOf[Index])
    )

    // Encode each Sample as a String in the Stream
    scalars.liftTo[Stream[IO, *]](
      LatisException("No non-Index scalars in model")
    ).flatMap { ss =>
      header(dataset).map(_ + "\r\n") ++ uncurriedDataset.samples.through(
        csv.encodeWithoutHeaders(
          fullRows = true,
          newline = "\r\n"
        )(sampleEncoder(ss))
      )
    }
  }

  // This assumes Index scalars have already been removed
  private def sampleEncoder(scalars: NonEmptyList[Scalar]): RowEncoder[Sample] =
    new RowEncoder[Sample] {
      override def apply(sample: Sample): Row = {
        // NOTE: Assuming there are the same number of sample elements
        // as scalars and that there is more than one scalar.
        val values = scalars.toList.zip(sample.domain ++ sample.range).map {
          case (s, d) => s.formatValue(d)
        }

        Row(NonEmptyList.fromListUnsafe(values))
      }
    }
}

object CsvEncoder {

  /** Default encoder with no header */
  def apply(): CsvEncoder = new CsvEncoder(_ => Stream())

  /**
   * Creates an encoder with the provided header. An empty string will create a
   * blank-line header.
   * @param header function that creates a header from a Dataset
   */
  def withHeader(header: Dataset => String): CsvEncoder = new CsvEncoder(ds => Stream(header(ds)))

  def withColumnName: CsvEncoder = {
    def header(dataset: Dataset): String =
      dataset.model.getScalars.filterNot(_.isInstanceOf[Index]).map(_.id.asString).mkString(",")
    CsvEncoder.withHeader(header)
  }
}

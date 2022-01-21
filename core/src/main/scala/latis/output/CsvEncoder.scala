package latis.output

import cats.effect.IO
import fs2.Stream

import latis.data.Data
import latis.data.Sample
import latis.dataset.Dataset
import latis.model.DataType
import latis.model.Index
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
    header(dataset).map(_ + "\r\n") ++ uncurriedDataset.samples
      .map(encodeSample(uncurriedDataset.model, _) + "\r\n")
  }

  /**
   * Encodes a single Sample to a String of comma separated values.
   */
  def encodeSample(model: DataType, sample: Sample): String = {
    // Note that the dataset has been uncurried so there are no nested functions.
    val scalars: List[Scalar] = model.getScalars.filterNot(_.isInstanceOf[Index])
    val datas: List[Data] = sample.domain ++ sample.range
    //TODO: assert that the lengths are the same? should be ensured earlier
    scalars.zip(datas).map { case (s, d) =>
      s.formatValue(d)
    }.mkString(",")
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

package latis.service.dap2

import cats.effect.IO
import fs2.Stream

import latis.dataset.Dataset
import latis.output.Encoder

class DataDdsEncoder extends Encoder[IO, Byte]{
  override def encode(dataset: Dataset): Stream[IO, Byte] =
    Stream.fromEither[IO](DataDds.fromDataset(dataset).map(_.asBytes())).flatten
}

package latis.service.dap2

import cats.effect.IO
import fs2.Stream

import latis.dataset.Dataset
import latis.output.Encoder

class DdsEncoder extends Encoder[IO, String]{
  override def encode(dataset: Dataset): Stream[IO, String] =
    Stream.fromEither[IO](Dds.fromDataset(dataset).map(_.asString))
}

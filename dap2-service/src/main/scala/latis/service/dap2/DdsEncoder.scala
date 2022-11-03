package latis.service.dap2

import cats.effect.IO
import fs2.Stream

import latis.dataset.Dataset
import latis.output.Encoder

class DdsEncoder extends Encoder[IO, String]{
  override def encode(dataset: Dataset): Stream[IO, String] =
    Stream.emit(Dds.fromDataset(dataset).fold(_.message, _.asString))
}

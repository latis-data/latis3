package latis.service.dap2

import cats.effect.IO
import fs2.Stream

import latis.dataset.Dataset
import latis.output.Encoder

class DasEncoder extends Encoder[IO, String]{
  override def encode(dataset: Dataset): Stream[IO, String] =
    Stream.emit(Das.fromDataset(dataset).asString)
}

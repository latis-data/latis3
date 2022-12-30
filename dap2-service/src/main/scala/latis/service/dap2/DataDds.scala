package latis.service.dap2

import cats.effect.IO
import fs2.Stream

import latis.dataset.Dataset
import latis.util.LatisException

final case class DataDds(dds: Dds, data: Array[Byte]) {
  def asBytes(): Stream[IO, Byte] = {
    val ddsString = dds.asString
    val separator = "\r\nData:\r\n"
    val binary = (ddsString + separator).getBytes()++data
    Stream.emits(binary)
  }
}

object DataDds {
  def fromDataset(dataset: Dataset): Either[LatisException, DataDds] = {
    val dds = Dds.fromDataset(dataset)
    val data = Array[Byte]()
    dds.map(DataDds(_, data))
  }
}
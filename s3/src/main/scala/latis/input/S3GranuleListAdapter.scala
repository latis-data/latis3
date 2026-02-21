package latis.input

import java.net.URI

import cats.effect.IO

import latis.data.*
import latis.model.*

class S3GranuleListAdapter(
  model: DataType,
  config: FileListAdapter.Config
) extends StreamingAdapter[String] {

  def recordStream(uri: URI): fs2.Stream[IO, String] = ???

  def parseRecord(r: String): Option[Sample] = ???
}

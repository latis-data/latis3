package latis.output

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import fs2.Stream
import io.circe.syntax.*
import org.http4s.ServerSentEvent

import latis.data.Sample
import latis.dataset.Dataset
import latis.metadata.Metadata
import latis.util.LatisException

class SseEncoder extends Encoder[IO, ServerSentEvent] {

  override def encode(dataset: Dataset): Stream[IO, ServerSentEvent] =
    Stream.emit(makeMetadataEvent(dataset.metadata)) ++
      dataset.samples.mapChunks(makeDataEvent(_).pure[Chunk]).handleErrorWith {
        case e: LatisException =>
          // NOTE: Making the assumption that LatisException messages
          // are safe to share with users.
          Stream.emit(makeErrorEvent(e.message))
        case _ => Stream.emit(makeErrorEvent("unspecified error"))
      }

  private def makeDataEvent(samples: Chunk[Sample]): ServerSentEvent =
    ServerSentEvent(
      samples.toList.asJson.noSpaces.some,
      "data".some
    )

  private def makeErrorEvent(message: String): ServerSentEvent =
    ServerSentEvent(message.some, "error".some)

  private def makeMetadataEvent(metadata: Metadata): ServerSentEvent =
    ServerSentEvent(
      metadata.properties.asJson.noSpaces.some,
      "metadata".some
    )
}

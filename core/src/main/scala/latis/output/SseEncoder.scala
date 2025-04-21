package latis.output

import scala.concurrent.duration.*

import cats.Id
import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import org.http4s.ServerSentEvent

import latis.data.Sample
import latis.dataset.Dataset
import latis.util.LatisException

class SseEncoder extends Encoder[IO, ServerSentEvent] {

  private val heartbeatEvent: ServerSentEvent =
    ServerSentEvent(comment = "".some)

  private val heartbeatPeriod: FiniteDuration = 30.seconds

  /** A stream of heartbeat messages emitted at some interval. */
  private val heartbeatStream: Stream[IO, ServerSentEvent] =
    Stream.awakeDelay[IO](heartbeatPeriod).as(heartbeatEvent)

  override def encode(dataset: Dataset): Stream[IO, ServerSentEvent] = {
    val events = (Stream.emit(makeMetadataEvent(dataset)) ++
      dataset.samples.mapChunks(makeDataEvent(_).pure[Chunk])).handleErrorWith {
        case e: LatisException =>
          // NOTE: Making the assumption that LatisException messages
          // are safe to share with users.
          Stream.emit(makeErrorEvent(e.message))
        case _ => Stream.emit(makeErrorEvent("unspecified error"))
      }

    // Non-deterministically merge a stream of heartbeat events
    // (produced at some cadence) with the stream of other events,
    // halting when the stream of other events halts.
    heartbeatStream.mergeHaltR(events)
  }

  private def makeDataEvent(samples: Chunk[Sample]): ServerSentEvent =
    ServerSentEvent(
      Json.obj("data" -> samples.toList.asJson).noSpaces.some,
      "data".some
    )

  private def makeErrorEvent(message: String): ServerSentEvent =
    ServerSentEvent(
      Json.obj("message" -> message.asJson).noSpaces.some,
      "error".some
    )

  private def makeMetadataEvent(ds: Dataset): ServerSentEvent =
    ServerSentEvent(
      MetadataEncoder[Id].encode(ds).map(_.noSpaces).compile.string.some,
      "metadata".some
    )
}

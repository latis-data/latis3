package latis.output

import scala.concurrent.duration.*

import cats.Id
import cats.effect.IO
import cats.effect.Outcome
import cats.effect.Ref
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import fs2.Stream
import io.circe.literal.*
import org.http4s.ServerSentEvent

import latis.data.Data
import latis.data.Data.IntValue
import latis.data.Sample
import latis.data.SeqFunction
import latis.data.StreamFunction
import latis.dataset.MemoizedDataset
import latis.dataset.TappedDataset
import latis.metadata.Metadata
import latis.model.DataType
import latis.model.Function
import latis.model.IntValueType
import latis.model.Scalar
import latis.ops.UnaryOperation
import latis.util.Identifier.*
import latis.util.LatisException

class SseEncoderSuite extends munit.CatsEffectSuite {

  private val dataset = {
    val model: DataType = Function.from(
      Scalar(id"a", IntValueType),
      Scalar(id"b", IntValueType)
    ).getOrElse(fail("Failed to construct model"))

    val data = SeqFunction(
      List(
        Sample(List(IntValue(1)), List(IntValue(1))),
        Sample(List(IntValue(2)), List(IntValue(2))),
        Sample(List(IntValue(3)), List(IntValue(3)))
      )
    )

    MemoizedDataset(
      Metadata(id"dataset"),
      model,
      data,
      List.empty
    )
  }

  // A dataset that never emits a sample.
  private val streamDataset = {
    val model: DataType = Function.from(
      Scalar(id"a", IntValueType),
      Scalar(id"b", IntValueType)
    ).getOrElse(fail("Failed to construct model"))

    val data = StreamFunction(Stream.never)

    TappedDataset(
      Metadata(id"dataset"),
      model,

      data,
      List.empty
    )
  }

  // Operation that will fail at runtime.
  private val makeItFail = new UnaryOperation {
    override def applyToData(
      data: Data,
      model: DataType
    ): Either[LatisException, Data] = LatisException("oops").asLeft

    override def applyToModel(
      model: DataType
    ): Either[LatisException, DataType] = model.asRight
  }

  test("encodes a dataset") {
    val md = MetadataEncoder[Id].encode(dataset).map(_.noSpaces).compile.string
    val expected: List[ServerSentEvent] = List(
      ServerSentEvent(md.some, "metadata".some),
      ServerSentEvent(json"""{"data":[[1,1]]}""".noSpaces.some, "data".some),
      ServerSentEvent(json"""{"data":[[2,2]]}""".noSpaces.some, "data".some),
      ServerSentEvent(json"""{"data":[[3,3]]}""".noSpaces.some, "data".some)
    )

    SseEncoder()
      .encode(dataset)
      // Filter out heartbeat events. This assumes that other messages
      // don't have comments defined.
      .filter(_.comment.isEmpty)
      .compile
      .toList
      .map(assertEquals(_, expected))
  }

  test("emits heartbeats") {

    // Count the number of heartbeat messages received.
    val test = Ref.of[IO, Int](0).flatMap { counter =>
      SseEncoder()
        .encode(streamDataset)
        .evalTap { event =>
          counter.update(_ + 1).whenA(event.comment.nonEmpty)
        }
        // Drop the metadata message
        .drop(1)
        .take(2)
        .compile
        .drain
        .flatMap(_ => counter.get)
    }

    // Wait for two heartbeat messages and check that the test action
    // succeeded with a count of two.
    TestControl.execute(test).flatMap { control =>
      for {
        _ <- control.tick
        _ <- control.advanceAndTick(30.seconds)
        _ <- control.advanceAndTick(30.seconds)
        _ <- control.results.assertEquals(Some(Outcome.succeeded(Id(2))))
      } yield ()
    }
  }

  test("emits error messages") {
    val expected = ServerSentEvent(
      json"""{"message":"oops"}""".noSpaces.some, "error".some
    )

    SseEncoder()
      .encode(dataset.withOperation(makeItFail))
      // Filter out heartbeat events. This assumes that other messages
      // don't have comments defined.
      .filter(_.comment.isEmpty)
      .compile
      .toList
      .map {
        case meta :: err :: Nil =>
          assertEquals(meta.eventType, "metadata".some)
          assertEquals(err, expected)
        case _ => fail("Unexpected messages")
      }
  }
}

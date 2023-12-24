package latis.input

import java.net.URI
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset

import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream

import latis.data.Data
import latis.data.Datum
import latis.data.Sample
import latis.data.StreamFunction
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.ops.Operation
import latis.time.Time
import latis.time.TimeFormat
import latis.util.ConfigLike
import latis.util.Duration
import latis.util.Identifier.*
import latis.util.LatisException

/**
 * An adapter for creating datasets of granule URIs generated for a
 * given time range and step.
 *
 * This adapter produces datasets of the form `time → uri` where the
 * values of `uri` are derived from the corresponding values of `time`
 * using a format string and are then resolved against the base URI.
 *
 * The scalar `time` must be a [[latis.time.Time Time]]. It must have
 * type `string` and its units must be consistent with an ISO 8601
 * time string.
 *
 * The scalar `uri` must have `uri` as its identifier and it must have
 * type `string`.
 *
 * The base URI is given by the `source` element in FDML or by the
 * [[java.net.URI URI]] given when calling [[getData]].
 *
 * The `pattern` configuration key defines the format string. The
 * format string is expected to follow the Java [[java.util.Formatter
 * Formatter]] format. The date and time conversions will be the most
 * useful. Because there is only one time per URI, each conversion
 * character must be prefixed with `1$` to work properly.
 *
 * The `start` and `end` configuration keys define the coverage
 * (inclusive on the end) of the generated dataset. These are expected
 * to be ISO 8601 strings in UTC. The `end` is optional and defaults
 * to being evaluated as the current time when data are requested.
 *
 * The `step` configuration key defines the time step between
 * generated samples. It is expected to be an ISO 8601 duration string
 * using only integers.
 */
class GranuleListGenerator private[input] (
  model: DataType,
  config: GranuleListGenerator.Config
) extends Adapter {

  // The baseUri is the URI to which the pattern will be appended
  override def getData(baseUri: URI, ops: Seq[Operation]): Data =
    validateModel(model) match {
      case Left(ex) => throw ex
      case Right((time, uri)) =>
        StreamFunction(enumerateTimes.map(makeSample(time, uri, baseUri, _)))
    }

  private def enumerateTimes: Stream[IO, LocalDateTime] = {
    val ioend = IO(config.end.getOrElse(LocalDateTime.now(ZoneId.of("Z"))))
    Stream.eval(ioend).flatMap { end =>
      Stream.iterate(config.start)(_.plus(config.step))
        .takeWhile(t => t.isBefore(end) || t.isEqual(end))
    }
  }

  private def makeSample(
    time: Time,
    uri: Scalar,
    baseUri: URI,
    t: LocalDateTime
  ): Sample = (for {
    d <- makeTime(time, t)
    r <- makeUri(uri, baseUri, t)
  } yield Sample(List(d), List(r))).fold(throw _, identity)

  private def makeTime(
    time: Time,
    t: LocalDateTime
  ): Either[LatisException, Datum] =
    time.parseValue(TimeFormat.Iso.format(t.toEpochSecond(ZoneOffset.UTC)*1000))

  private def makeUri(
    uri: Scalar,
    baseUri: URI,
    t: LocalDateTime
  ): Either[LatisException, Data] =
    uri.parseValue(baseUri.resolve(config.pattern.format(t)).toString)

  // Checks that the model given is a function from time to a scalar
  // called "uri", otherwise returns a LatisException which will
  // eventually be thrown.
  private def validateModel(
    model: DataType
  ): Either[LatisException, (Time, Scalar)] = model match {
    case Function(t: Time, u: Scalar) if u.id == id"uri" => (t, u).asRight
    case _ => LatisException(s"Unsupported model: $model").asLeft
  }
}

object GranuleListGenerator extends AdapterFactory {

  override def apply(
    model: DataType,
    config: AdapterConfig
  ): GranuleListGenerator =
    Config.fromConfigLike(config).fold(
      throw _,
      new GranuleListGenerator(model, _)
    )

  private[input] final case class Config(
    start: LocalDateTime,
    end: Option[LocalDateTime],
    step: Duration,
    pattern: String
  )

  private[input] object Config {
    def fromConfigLike(cl: ConfigLike): Either[LatisException, Config] = for {
      start   <- cl.get("start")
                   .toRight(LatisException("Adapter requires a start time"))
                   .flatMap(parseTime)
      end     <- cl.get("end").traverse(parseTime)
      _       <- if (end.exists(start.isAfter))
                   LatisException("End must come after start").asLeft
                 else ().asRight
      step    <- cl.get("step")
                   .toRight(LatisException("Adapter requires a step."))
                   .flatMap(Duration.fromIsoString)
      pattern <- cl.get("pattern")
                   .toRight(LatisException("Adapter requires a pattern."))
    } yield Config(start, end, step, pattern)

    private def parseTime(str: String): Either[LatisException, LocalDateTime] =
      TimeFormat.parseIso(str).map { ms =>
        LocalDateTime.ofEpochSecond(ms/1000, 0, ZoneOffset.UTC)
      }
  }
}

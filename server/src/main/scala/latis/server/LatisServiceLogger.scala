package latis.server

import java.util.UUID
import java.util.concurrent.TimeUnit

import cats.data.Kleisli
import cats.effect.Clock
import cats.effect.Sync
import cats.effect.Timer
import cats.syntax.all._
import io.chrisdavenport.log4cats.StructuredLogger
import org.http4s.HttpApp
import org.http4s.Response
import org.http4s.Request
import org.http4s.server.middleware.{Logger => Http4sLogger}

/**
 * Middleware that logs requests and responses (without bodies) and
 * the time elapsed between receiving the request and starting the
 * response.
 */
object LatisServiceLogger {

  def apply[F[_]: Sync: Timer](
    app: HttpApp[F],
    logger: StructuredLogger[F]
  ): HttpApp[F] = Kleisli { req =>
    for {
      id       <- Sync[F].delay(UUID.randomUUID().toString())
      ctxLogger = StructuredLogger.withContext(logger)(Map("request-id" -> id))
      _        <- Http4sLogger.logMessage[F, Request[F]](req)(
        logHeaders = true, logBody = false
      )(ctxLogger.info(_))
      t0       <- Clock[F].monotonic(TimeUnit.MILLISECONDS)
      res      <- app(req)
      t1       <- Clock[F].monotonic(TimeUnit.MILLISECONDS)
      _        <- Http4sLogger.logMessage[F, Response[F]](res)(
        logHeaders = true, logBody = false
      )(ctxLogger.info(_))
      elapsed   = t1 - t0
      _        <- ctxLogger.info(s"Elapsed (ms): $elapsed")
    } yield res
  }
}

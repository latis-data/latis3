package latis.server

import java.util.UUID

import cats.data.Kleisli
import cats.effect.Async
import cats.effect.Clock
import cats.effect.Sync
import cats.syntax.all._
import fs2.Stream
import org.http4s.HttpApp
import org.http4s.Request
import org.http4s.Response
import org.http4s.server.middleware.{Logger => Http4sLogger}
import org.typelevel.log4cats.StructuredLogger

/**
 * Middleware that logs requests and responses (without bodies), time
 * elapsed between receiving the request and finishing the response,
 * and exceptions thrown during the response.
 */
object LatisServiceLogger {

  def apply[F[_]: Async](
    app: HttpApp[F],
    logger: StructuredLogger[F]
  ): HttpApp[F] = Kleisli { req =>
    for {
      id       <- Sync[F].delay(UUID.randomUUID().toString())
      ctxLogger = StructuredLogger.withContext(logger)(Map("request-id" -> id))
      _        <- Http4sLogger.logMessage[F, Request[F]](req)(
        logHeaders = true, logBody = false
      )(ctxLogger.info(_))
      t0       <- Clock[F].monotonic
      res      <- app(req)
      timedBody = res.body ++ Stream.exec {
        Clock[F].monotonic.flatMap { t1 =>
          val elapsed = t1 - t0
          ctxLogger.info(s"Elapsed (ms): ${elapsed.toMillis}")
        }
      }
      errBody   = timedBody.handleErrorWith { err =>
        Stream.exec(ctxLogger.error(err)("Exception thrown during response")) ++
        Stream.raiseError(err)
      }
      res2      = res.copy(body = errBody)
      _        <- Http4sLogger.logMessage[F, Response[F]](res2)(
        logHeaders = true, logBody = false
      )(ctxLogger.info(_))
    } yield res2
  }
}

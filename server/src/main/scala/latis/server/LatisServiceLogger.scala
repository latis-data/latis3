package latis.server

import cats.Monad
import cats.data.Kleisli
import cats.effect.Clock
import cats.effect.Timer
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.http4s.HttpApp
import org.http4s.Response
import org.http4s.Request
import java.util.concurrent.TimeUnit

/**
 * Middleware that logs requests and responses (without bodies) and
 * the time elapsed between receiving the request and starting the
 * response.
 */
object LatisServiceLogger {

  def apply[F[_]: Monad: Timer](
    app: HttpApp[F],
    logger: Logger[F]
  ): HttpApp[F] = Kleisli { req =>
    for {
      t0  <- Clock[F].monotonic(TimeUnit.MILLISECONDS)
      res <- app(req)
      t1  <- Clock[F].monotonic(TimeUnit.MILLISECONDS)
      elapsed = t1 - t0
      _   <- logger.info(makeMessage(req, res, elapsed))
    } yield res
  }

  private def makeMessage[F[_]](
    req: Request[F],
    res: Response[F],
    elapsed: Long
  ): String = {
    val reqMsg = req match {
      case Request(method, uri, version, headers, _, _) =>
        s"$version $method $uri $headers"
    }

    val resMsg = res match {
      case Response(status, version, headers, _, _) =>
        s"$version $status $headers"
    }

    s"Request: {$reqMsg} Response: {$resMsg} Elapsed (ms): $elapsed"
  }
}

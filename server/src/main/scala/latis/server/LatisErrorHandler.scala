package latis.server

import scala.util.control.NonFatal

import cats.MonadThrow
import cats.data.Kleisli
import cats.mtl.Ask
import cats.syntax.all.*
import org.http4s.EntityEncoder
import org.http4s.Headers
import org.http4s.HttpApp
import org.http4s.Response
import org.http4s.Status
import org.http4s.headers.Connection
import org.typelevel.ci.*
import org.typelevel.log4cats.StructuredLogger

/**
 * Catches otherwise unhandled non-fatal exceptions, logs them, and
 * returns a 500 response with the message in the body.
 *
 * This only handles errors raised in the process of constructing the
 * response, not errors raised while streaming the response.
 */
object LatisErrorHandler {

  def apply[F[_]: MonadThrow](
    app: HttpApp[F],
    logger: StructuredLogger[F]
  )(using F: Ask[F, Map[String, String]]): HttpApp[F] = Kleisli { req =>
    app.run(req).recoverWith {
      case NonFatal(err) =>
        F.ask.flatMap { ctx =>
          logger.error(ctx, err)("Request failed")
        } *>
        F.applicative.pure(
          Response(
            Status.InternalServerError,
            req.httpVersion,
            Headers(Connection(ci"close")),
            EntityEncoder[F, String].toEntity(err.getMessage()).body
          )
        )
    }
  }
}

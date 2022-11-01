package latis.lambda

import cats.data.Kleisli
import cats.data.OptionT
import cats.effect.Async
import cats.effect.Clock
import cats.effect.IO
//import feral.lambda.LambdaEnv
import org.http4s.HttpRoutes
import org.http4s.Request
import org.http4s.Response
// import org.http4s.server.middleware.{Logger => Http4sLogger}
import org.typelevel.log4cats.StructuredLogger

object LatisLambdaLogger {

  def apply[F[_]: Async](
    routes: HttpRoutes[F],
    logger: StructuredLogger[F],
    id: String
  ): HttpRoutes[F] = {
    // val ctxLogger = StructuredLogger.withContext(logger)(Map("request-id" -> id))

    // Kleisli { req =>
    //   for {
    //     _       <- OptionT.liftF(
    //       Http4sLogger.logMessage[F, Request[F]](req)(
    //         logHeaders = true, logBody = false
    //       )(ctxLogger.info(_))
    //     )
    //     t0      <- OptionT.liftF(Clock[F].monotonic)
    //     res     <- routes(req)
    //     t1      <- OptionT.liftF(Clock[F].monotonic)
    //     _       <- OptionT.liftF(
    //       Http4sLogger.logMessage[F, Response[F]](res)(
    //         logHeaders = true, logBody = false
    //       )(ctxLogger.info(_))
    //     )
    //     elapsed  = t1 - t0
    //     _       <- OptionT.liftF(
    //       ctxLogger.info(s"Elapsed (ms): ${elapsed.toMillis}")
    //     )
    //   } yield res
    // }

    ???
  }
}

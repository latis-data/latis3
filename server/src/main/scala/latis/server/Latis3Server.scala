package latis.server

import cats.effect._
import cats.implicits._
import org.http4s.implicits._
import org.http4s.server.blaze._
import org.http4s.server.Router
import pureconfig.generic.auto._
import pureconfig.module.catseffect._

import latis.service.dap2.Dap2Service

object Latis3Server extends IOApp {

  val config: IO[ServerConfig] =
    loadConfigF[IO, ServerConfig]

  def startServer(conf: ServerConfig): IO[ExitCode] =
    conf match {
      case ServerConfig(port, mapping) =>
        BlazeServerBuilder[IO]
          .bindHttp(port, "0.0.0.0")
          .withHttpApp {
            Router(mapping -> new Dap2Service[IO].service).orNotFound
          }
          .serve
          .compile
          .drain
          .as(ExitCode.Success)
    }

  override def run(args: List[String]): IO[ExitCode] =
    config >>= startServer
}

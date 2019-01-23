package latis.server

import cats.effect._
import cats.implicits._
import org.http4s.implicits._
import org.http4s.server.blaze._
import org.http4s.server.Router

import latis.service.dap2.Dap2Service

object Latis3Server extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    BlazeServerBuilder[IO]
      .bindHttp(8080, "0.0.0.0")
      .withHttpApp(new Dap2Service[IO].service.orNotFound)
      .serve
      .compile
      .drain
      .as(ExitCode.Success)
}

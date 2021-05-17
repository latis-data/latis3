package latis.server

import cats.effect.Blocker
import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Resource
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._

import latis.catalog.FdmlCatalog

import Latis3ServerBuilder._

object Latis3Server extends IOApp {

  private val loader: ServiceInterfaceLoader =
    new ServiceInterfaceLoader()

  private def getServiceConf(blocker: Blocker): IO[ServiceConf] =
    latisConfigSource.loadF[IO, ServiceConf](blocker)

  def run(args: List[String]): IO[ExitCode] =
    (for {
      logger      <- Resource.eval(Slf4jLogger.create[IO])
      blocker     <- Blocker[IO]
      serverConf  <- Resource.eval(getServerConf(blocker))
      catalogConf <- Resource.eval(getCatalogConf(blocker))
      catalog      = FdmlCatalog.fromDirectory(
        catalogConf.dir,
        catalogConf.validate
      )
      serviceConf <- Resource.eval(getServiceConf(blocker))
      interfaces  <- Resource.eval(loader.loadServices(serviceConf, catalog))
      server      <- mkServer(serverConf, interfaces, logger)
    } yield server)
      .use(_ => IO.never)
      .as(ExitCode.Success)
}

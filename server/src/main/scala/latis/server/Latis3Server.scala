package latis.server

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

  private val getServiceConf: IO[ServiceConf] =
    latisConfigSource.loadF[IO, ServiceConf]()

  def run(args: List[String]): IO[ExitCode] =
    (for {
      logger      <- Resource.eval(Slf4jLogger.create[IO])
      serverConf  <- Resource.eval(getServerConf)
      catalogConf <- Resource.eval(getCatalogConf)
      catalog     <- Resource.eval(
        FdmlCatalog.fromDirectory(catalogConf.dir, catalogConf.validate)
      )
      serviceConf <- Resource.eval(getServiceConf)
      interfaces  <- Resource.eval(loader.loadServices(serviceConf, catalog))
      server      <- mkServer(serverConf, interfaces, logger)
    } yield server)
      .use(_ => IO.never)
      .as(ExitCode.Success)
}

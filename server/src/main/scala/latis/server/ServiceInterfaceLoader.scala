package latis.server

import java.lang.ClassLoader
import java.net.URL
import java.net.URLClassLoader

import cats.effect.IO
import cats.syntax.all.*

import latis.catalog.Catalog
import latis.ops.OperationRegistry

final class ServiceInterfaceLoader {

  /**
   * Load service interfaces described in the service interface
   * configuration.
   *
   * This only needs to be called once on server initialization.
   */
  def loadServices(
    conf: ServiceConf,
    catalog: Catalog,
    operationRegistry: OperationRegistry
  ): IO[List[(String, ServiceInterface)]] =
    conf.services.traverse { spec =>
      for {
        loader  <- getClassLoader(spec)
        service <- loadService(loader, spec.clss, catalog, operationRegistry)
      } yield (spec.prefix, service)
    }

  private[server] def loadService(
    cl: ClassLoader,
    name: String,
    catalog: Catalog,
    operationRegistry: OperationRegistry
  ): IO[ServiceInterface] =
    IO {
      cl.loadClass(name).getConstructor(
        classOf[Catalog],
        classOf[OperationRegistry]
      ).newInstance(catalog, operationRegistry).asInstanceOf[ServiceInterface]
    }

  private def getClassLoader(spec: ServiceSpec): IO[ClassLoader] = spec match {
    case _: ClassPathServiceSpec =>
      IO(Thread.currentThread().getContextClassLoader())
    case spec: JarServiceSpec    =>
      List(spec.path).pure[IO].flatMap(mkClassLoader)
  }

  private def mkClassLoader(artifacts: List[URL]): IO[ClassLoader] = IO {
    val parent = Thread.currentThread().getContextClassLoader()
    new URLClassLoader(artifacts.toArray, parent)
  }
}

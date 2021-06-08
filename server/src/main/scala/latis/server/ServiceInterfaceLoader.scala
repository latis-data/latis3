package latis.server

import java.lang.ClassLoader
import java.net.URL
import java.net.URLClassLoader

import scala.reflect.runtime.{ universe => ru }

import cats.effect.ContextShift
import cats.effect.IO
import cats.syntax.all._

import latis.catalog.Catalog

final class ServiceInterfaceLoader(implicit cs: ContextShift[IO]) {

  /**
   * Load service interfaces described in the service interface
   * configuration.
   *
   * This only needs to be called once on server initialization.
   */
  def loadServices(
    conf: ServiceConf,
    catalog: Catalog
  ): IO[List[(String, ServiceInterface)]] =
    conf.services.traverse { spec =>
      for {
        loader  <- getClassLoader(spec)
        service <- loadService(loader, spec, catalog)
      } yield (spec.mapping, service)
    }

  private def loadService(
    cl: ClassLoader,
    spec: ServiceSpec,
    catalog: Catalog
  ): IO[ServiceInterface] =
    IO {
      val constructor = {
        val m = ru.runtimeMirror(cl)
        val clss = m.staticClass(spec.clss)
        val cm = m.reflectClass(clss)
        val ctor = clss.toType.decl(ru.termNames.CONSTRUCTOR).asMethod
        cm.reflectConstructor(ctor)
      }
      constructor(catalog).asInstanceOf[ServiceInterface]
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

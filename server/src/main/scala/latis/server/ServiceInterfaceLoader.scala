package latis.server

import java.lang.ClassLoader
import java.net.URL
import java.net.URLClassLoader

import scala.reflect.runtime.{ universe => ru }

import cats.effect.ContextShift
import cats.effect.IO
import cats.implicits._
import coursier._
import coursier.cache.FileCache
import coursier.interop.cats._

final class ServiceInterfaceLoader(implicit cs: ContextShift[IO]) {

  // Make Coursier use cats-effect IO.
  private val cache: FileCache[IO] = FileCache[IO]()

  /**
   * Load service interfaces described in the service interface
   * configuration.
   *
   * This method downloads the service interface artifact (and its
   * dependencies) and constructs an instance of the service
   * interface.
   *
   * This only needs to be called once on server initialization.
   */
  def loadServices(conf: ServiceConf): IO[List[(ServiceSpec, ServiceInterface)]] =
    conf.services.traverse { spec =>
      for {
        loader  <- getClassLoader(spec)
        service <- loadService(loader, spec)
      } yield (spec, service)
    }

  private def loadService(cl: ClassLoader, spec: ServiceSpec): IO[ServiceInterface] =
    IO {
      val constructor = {
        val m = ru.runtimeMirror(cl)
        val clss = m.staticClass(spec.clss)
        val cm = m.reflectClass(clss)
        val ctor = clss.toType.decl(ru.termNames.CONSTRUCTOR).asMethod
        cm.reflectConstructor(ctor)
      }
      constructor().asInstanceOf[ServiceInterface]
    }

  private def fetchServiceArtifacts(spec: MavenServiceSpec): IO[List[URL]] = {
    val dep = {
      val nameM = ModuleName(s"${spec.name}_2.12")
      Dependency(Module(org"io.latis-data", nameM), spec.version)
    }

    Fetch(cache).addDependencies(dep).io.map(_.toList.map(_.toURI().toURL()))
  }

  private def getClassLoader(spec: ServiceSpec): IO[ClassLoader] = spec match {
    case _: ClassPathServiceSpec =>
      IO(Thread.currentThread().getContextClassLoader())
    case spec: MavenServiceSpec  =>
      fetchServiceArtifacts(spec).flatMap(mkClassLoader)
    case spec: JarServiceSpec    =>
      List(spec.path).pure[IO].flatMap(mkClassLoader)
  }

  private def mkClassLoader(artifacts: List[URL]): IO[ClassLoader] = IO {
    val parent = Thread.currentThread().getContextClassLoader()
    new URLClassLoader(artifacts.toArray, parent)
  }
}

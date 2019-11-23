package latis.server

import java.io.File
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
  val cache: FileCache[IO] = FileCache[IO]()

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
    for {
      artifacts <- fetchServiceArtifacts(conf)
      cl        <- makeClassLoader(artifacts)
      services  <- conf.services.traverse { spec =>
        loadService(cl, spec).map((spec, _))
      }
    } yield services

  private def loadService(cl: URLClassLoader, spec: ServiceSpec): IO[ServiceInterface] =
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

  private def fetchServiceArtifacts(conf: ServiceConf): IO[List[File]] = {
    val dependencies: List[Dependency] =
      conf.services.map {
        case ServiceSpec(name, version, _, _) =>
          val nameM = ModuleName(s"${name}_2.12")
          Dependency(Module(org"io.latis-data", nameM), version)
      }

    Fetch(cache).withDependencies(dependencies).io.map(_.toList)
  }

  private def makeClassLoader(paths: List[File]): IO[URLClassLoader] =
    IO {
      new URLClassLoader(
        paths.map(_.toURI().toURL()).toArray,
        Thread.currentThread().getContextClassLoader()
      )
    }
}

package latis.server

import java.io.File
import java.net.URLClassLoader

import scala.reflect.runtime.{ universe => ru }

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.implicits._
import coursier._
import coursier.cache.FileCache
import coursier.interop.cats._
import org.http4s.HttpRoutes
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.blaze._
import pureconfig.generic.auto._
import pureconfig.module.catseffect._

object Latis3Server extends IOApp {

  final case class ServerConf(
    port: Int,
    mapping: String
  )

  final case class ServiceSpec(
    name: String,
    version: String,
    mapping: String,
    clss: String
  )

  final case class ServiceConf(
    services: List[ServiceSpec]
  )

  // Make Coursier use cats-effect IO.
  val cache: FileCache[IO] = FileCache()

  val getServerConf: IO[ServerConf] =
    loadConfigF[IO, ServerConf]

  val getServiceConf: IO[ServiceConf] =
    loadConfigF[IO, ServiceConf]

  def loadService(cl: URLClassLoader, spec: ServiceSpec): IO[ServiceInterface] =
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

  def fetchServiceArtifacts(conf: ServiceConf): IO[List[File]] = {
    val dependencies: List[Dependency] =
      conf.services.map {
        case ServiceSpec(name, version, _, _) =>
          val nameM = ModuleName(s"${name}_2.12")
          Dependency.of(Module(org"io.latis-data", nameM), version)
      }

    Fetch(cache).withDependencies(dependencies).io.map(_.toList)
  }

  def getClassLoader(paths: List[File]): IO[URLClassLoader] =
    IO {
      new URLClassLoader(
        paths.map(_.toURI().toURL()).toArray,
        Thread.currentThread().getContextClassLoader()
      )
    }

  def loadServices(conf: ServiceConf): IO[List[(ServiceSpec, ServiceInterface)]] =
    for {
      artifacts <- fetchServiceArtifacts(conf)
      cl        <- getClassLoader(artifacts)
      services  <- conf.services.traverse { spec =>
        loadService(cl, spec).map((spec, _))
      }
    } yield services

  def constructRoutes(services: List[(ServiceSpec, ServiceInterface)]): HttpRoutes[IO] = {
    val routes: List[(String, HttpRoutes[IO])] = services.map {
      case (ServiceSpec(_, _, mapping, _), service) => (mapping, service.routes)
    }
    Router(routes:_*)
  }

  def startServer(routes: HttpRoutes[IO], conf: ServerConf): IO[Unit] =
    conf match {
      case ServerConf(port, mapping) =>
        BlazeServerBuilder[IO]
          .bindHttp(port, "0.0.0.0")
          .withHttpApp {
            Router(mapping -> routes).orNotFound
          }
          .serve
          .compile
          .drain
    }

  def run(args: List[String]): IO[ExitCode] =
    for {
      serverConf  <- getServerConf
      serviceConf <- getServiceConf
      services    <- loadServices(serviceConf)
      routes       = constructRoutes(services)
      _           <- startServer(routes, serverConf)
    } yield ExitCode.Success
}

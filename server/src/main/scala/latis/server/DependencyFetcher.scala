package latis.server

import java.io.File
import java.net.URL
import java.net.URLClassLoader

import cats.effect.ContextShift
import cats.effect.IO
import cats.implicits._
import coursier.Dependency
import coursier.Fetch
import coursier.maven.MavenRepository
import coursier.Module
import coursier.ModuleName
import coursier.Organization
import coursier.Repository
import coursier.cache.FileCache
import coursier.interop.cats._

/** TODO */
final class DependencyFetcher(
  repositoryConf: RepositoryConf,
  dependencyConf: DependencyConf
)(
  implicit cs: ContextShift[IO]
) {

  private val cache: FileCache[IO] = FileCache[IO]()

  /** TODO */
  val fetch: IO[ClassLoader] = dependencyConf
    .dependencies
    .flatTraverse(fetchDependency)
    .flatMap(mkClassLoader)

  private def mkClassLoader(artifacts: List[URL]): IO[ClassLoader] = IO {
    val parent = Thread.currentThread().getContextClassLoader()
    new URLClassLoader(artifacts.toArray, parent)
  }

  private def fetchDependency(dep: DependencySpec): IO[List[URL]] = dep match {
    case d: MavenDependencySpec => fetchMavenDependency(d)
    case d: JarDependencySpec   => List(d.url).pure[IO]
  }

  private def fetchMavenDependency(dep: MavenDependencySpec): IO[List[URL]] =
    Fetch(cache)
      .addRepositories(repositoryConf.repositories.map(toRepository): _*)
      .addDependencies(toDependency(dep))
      .io
      .map(_.toList.map(toURL))

  private def toDependency(d: MavenDependencySpec): Dependency = d match {
    case MavenDependencySpec(g, a, v) =>
      Dependency(Module(Organization(g), ModuleName(a)), v)
  }

  private def toRepository(url: URL): Repository =
    MavenRepository(url.toString)

  private def toURL(f: File): URL = f.toURI().toURL()
}

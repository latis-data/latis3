package latis.catalog

import java.net.URL

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import fs2.io.file._

import latis.dataset.Dataset
import latis.input.fdml.FdmlReader
import latis.util.LatisException

object FdmlCatalog {

  /**
   * Creates a catalog from a directory of FDML files.
   *
   * This catalog will only include files with the "fdml" extension
   * and does not recurse into subdirectories.
   */
  def fromDirectory(path: Path, validate: Boolean = true): IO[Catalog] =
    dirDatasetStream(path, validate)
      .compile
      .toVector
      .map(Catalog.fromFoldable(_))

  /**
   * Creates a catalog from a directory of FDML files on the
   * classpath.
   *
   * This catalog will only include files with the "fdml" extension
   * and does not recurse into subdirectories.
   */
  def fromClasspath(
    cl: ClassLoader,
    path: Path,
    validate: Boolean = true
  ): IO[Catalog] =
    for {
      url <- getResource(cl, path.toString)
      dir <- IO.fromEither(urlToPath(url))
      dss <- dirDatasetStream(dir, validate).compile.toVector
    } yield Catalog.fromFoldable(dss)

  private def dirDatasetStream(dir: Path, validate: Boolean): Stream[IO, Dataset] =
    Files[IO].list(dir, "*.fdml").flatMap { f =>
      pathToDataset(f, validate).fold(
        t => {
          println(s"[WARN] Fdml dataset dropped: $f. $t") //TODO: log
          Stream.empty
        },
        Stream.emit
      )
    }

  private def getResource(
    cl: ClassLoader,
    resource: String
  ): IO[URL] = IO(Option(cl.getResource(resource))).flatMap {
    case None      => IO.raiseError(
      LatisException(s"Unable to load resource: $resource")
    )
    case Some(url) => IO.pure(url)
  }

  private def pathToDataset(path: Path, validate: Boolean): Either[Throwable, Dataset] =
    Either.catchNonFatal {
      FdmlReader.read(path.toNioPath.toUri(), validate)
    }

  private def urlToPath(url: URL): Either[Throwable, Path] =
    Either.catchNonFatal(Path(url.getPath))
}

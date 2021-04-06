package latis.catalog

import java.net.URL
import java.nio.file.Path
import java.nio.file.Paths

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import fs2.io.file.directoryStream

import latis.dataset.Dataset
import latis.input.fdml.FdmlReader
import latis.util.StreamUtils

import StreamUtils.contextShift

object FdmlCatalog {
  /**
   * Creates a catalog from a directory of FDML files.
   *
   * This catalog will only include files with the "fdml" extension
   * and does not recurse into subdirectories.
   */
  def fromDirectory(path: Path, validate: Boolean = true): Catalog = new Catalog {
    override val datasets: Stream[IO, Dataset] = dirDatasetStream(path, validate)
  }

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
  ): Catalog = new Catalog {
    override val datasets: Stream[IO, Dataset] = for {
      url <- Stream.eval(getResource(cl, path.toString())).unNone
      dir <- Stream.fromEither[IO](urlToPath(url))
      ds  <- dirDatasetStream(dir, validate)
    } yield ds

    private def getResource(
      cl: ClassLoader,
      resource: String
    ): IO[Option[URL]] = IO(Option(cl.getResource(resource)))

    private def urlToPath(url: URL): Either[Throwable, Path] =
      Either.catchNonFatal(Paths.get(url.toURI()))
  }

  private def dirDatasetStream(dir: Path, validate: Boolean): Stream[IO, Dataset] =
    directoryStream[IO](StreamUtils.blocker, dir, "*.fdml").flatMap { f =>
      // TODO: Log failures to read datasets.
      pathToDataset(f, validate).fold(_ => Stream.empty, Stream.emit)
    }

  private def pathToDataset(path: Path, validate: Boolean): Either[Throwable, Dataset] =
    Either.catchNonFatal {
      FdmlReader.read(path.toUri(), validate)
    }
}

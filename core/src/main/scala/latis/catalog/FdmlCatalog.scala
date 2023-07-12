package latis.catalog

import java.net.URL

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import fs2.io.file._

import latis.dataset.Dataset
import latis.input.fdml.FdmlReader
import latis.ops.OperationRegistry
import latis.util.LatisException

object FdmlCatalog {

  /**
   * Creates a catalog from a directory of FDML files.
   *
   * This catalog will only include files with the "fdml" extension
   * and does not recurse into subdirectories.
   */
  def fromDirectory(
    path: Path,
    validate: Boolean = true,
    opReg: OperationRegistry = OperationRegistry.default
  ): IO[Catalog] =
    dirDatasetStream(path, validate, opReg)
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
    validate: Boolean = true,
    opReg: OperationRegistry = OperationRegistry.default
  ): IO[Catalog] =
    for {
      url <- getResource(cl, path.toString)
      dir <- IO.fromEither(urlToPath(url))
      dss <- dirDatasetStream(dir, validate, opReg).compile.toVector
    } yield Catalog.fromFoldable(dss)

  private def dirDatasetStream(dir: Path, validate: Boolean, opReg: OperationRegistry): Stream[IO, Dataset] =
    Files[IO].list(dir, "*.fdml").flatMap { f =>
      pathToDataset(f, validate, opReg).fold(
        t => {
          Stream.eval(IO.println(s"[WARN] Fdml dataset dropped: $f. $t")) >> //TODO: log
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

  private def pathToDataset(path: Path, validate: Boolean, opReg: OperationRegistry): Either[Throwable, Dataset] =
    Either.catchNonFatal {
      FdmlReader.read(path.toNioPath.toUri(), validate, opReg)
    }

  private def urlToPath(url: URL): Either[Throwable, Path] =
    Either.catchNonFatal(Path(url.getPath))
}

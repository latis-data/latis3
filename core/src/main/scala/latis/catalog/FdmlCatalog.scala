package latis.catalog

import java.nio.file.Path

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
    override val datasets: Stream[IO, Dataset] =
      directoryStream[IO](StreamUtils.blocker, path, "*.fdml").flatMap { f =>
        // TODO: Log failures to read datasets.
        pathToDataset(f, validate).fold(_ => Stream.empty, Stream.emit)
      }
  }

  private def pathToDataset(path: Path, validate: Boolean): Either[Throwable, Dataset] =
    Either.catchNonFatal {
      FdmlReader.read(path.toUri(), validate)
    }
}

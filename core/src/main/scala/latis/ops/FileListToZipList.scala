package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.Pipe
import fs2.Pull
import fs2.Pure
import fs2.Stream
import fs2.io.file.Files
import fs2.io.file.Path
import org.http4s.Uri

import latis.data.Data.BinaryValue
import latis.data.Data.StringValue
import latis.data.*
import latis.model.*
import latis.util.Identifier.*
import latis.util.LatisException

/**
 * An operation that produces a ZIP list dataset (`entryName → bytes`)
 * out of a file list dataset.
 *
 * It is assumed that the URIs from the file list dataset are file
 * URIs. They may be relative URIs if the `baseUri` metadata property
 * is set on `uri`.
 *
 * The values of `entryName` will be the final path segment of the
 * corresponding URI. A numeric suffix will be added between the name
 * and extension (if present) if a name would otherwise conflict with
 * an existing name.
 */
final class FileListToZipList private[ops] (
  fetchBytes: Path => IO[Array[Byte]]
) extends UnaryOperation {

  override def applyToData(
    data: Data,
    model: DataType
  ): Either[LatisException, Data] = model match {
    case Function(_, range) =>
      range.findVariable(id"uri").asRight.flatMap {
        case Some(s: Scalar) if s.valueType == StringValueType =>
          // This should be safe because we've verified there's a uri
          // scalar by this point.
          val pos = range.findPath(id"uri").getOrElse(
            throw LatisException("No sample path for scalar we already found")
          )

          s.metadata.getProperty("baseUri").asRight.flatMap { baseUriStr =>
            (baseUriStr match {
              case None => Option.empty.asRight
              case Some(str) => Uri.fromString(str).bimap(
                _ => LatisException("Failed to parse base URI"),
                _.some
              )
            }).map { baseUri =>
              val samples = data.samples.through(applyToSamples(baseUri, pos))
              StreamFunction(samples)
            }
          }
        case _ => LatisException("Unsupported model").asLeft
      }
    case _ => LatisException("Unsupported model").asLeft
  }

  // The model after applying this operation is entryName → bytes.
  override def applyToModel(
    model: DataType
  ): Either[LatisException, DataType] =
    Function.from(
      Scalar(id"entryName", StringValueType),
      Scalar(id"bytes", BinaryValueType)
    )

  private def applyToSamples(
    baseUri: Option[Uri],
    uriPos: SamplePath
  ): Pipe[IO, Sample, Sample] = {
    def go(
      usedNames: Set[String],
      s: Stream[IO, Sample]
    ): Pull[IO, Sample, Unit] = s.pull.uncons1.flatMap {
      case Some((sample, tl)) =>
        // Should be safe to get head of sample path because we know
        // the uri scalar is in there.
        sample.getValue(uriPos.head) match {
          case Some(Text(uri)) =>
            Uri.fromString(uri).map { uri =>
              val resolved = baseUri.map(_.resolve(uri)).getOrElse(uri)
              val path = Path(resolved.path.toString())
              val (newUsed, entryName) = deriveEntryName(usedNames, path)

              Pull.eval(fetchBytes(path)).map { bytes =>
                Sample(List(StringValue(entryName)), List(BinaryValue(bytes)))
              }.flatMap(Pull.output1) >> go(newUsed, tl)
            }.getOrElse {
              Pull.raiseError[IO](LatisException("Failed to parse URI"))
            }
          case _ => Pull.raiseError[IO](LatisException("Unsupported sample"))
        }
      case None => Pull.done
    }

    go(Set.empty, _).stream
  }

  /**
   * Derives unique entry names from paths.
   *
   * The entry name will be the final segment of the path. It is
   * assumed that a path here will have a final segment because it
   * should be referring to a file.
   *
   * Entry names are made unique by adding a numeric suffix between
   * the name and extension (if present).
   */
  private def deriveEntryName(
    used: Set[String],
    path: Path
  ): (Set[String], String) = {
    val unique = {
      val name = entryNameFromPath(path)

      if (used.contains(name)) {
        generatePossibleNames(name)
          .find(! used.contains(_))
          .compile
          .last
          .get
      } else name
    }

    (used + unique, unique)
  }

  /**
   * Return the last segment of a Path.
   *
   * It is assumed that a path here will have a final segment because
   * it should be referring to a file.
   */
  private def entryNameFromPath(path: Path): String =
    // Big problems if the path is empty.
    path.names.lastOption.map(_.toString).getOrElse(
      throw LatisException("Got an empty Path")
    )

  /**
   * Make a stream of candidate names with increasing numeric
   * suffixes.
   */
  private def generatePossibleNames(
    entryName: String
  ): Stream[Pure, String] = {
    splitEntryName(entryName) match {
      case (base, ext) => Stream.iterate(1)(_ + 1).map { n =>
        ext.fold(f"$base%s_$n%03d")(ext => f"$base%s_$n%03d.$ext%s")
      }
    }
  }

  /** Separate an entry name into name and extension components. */
  private def splitEntryName(
    entryName: String
  ): (String, Option[String]) = {
    entryName.split('.').toList.filter(_.nonEmpty).reverse match {
      case _ :: Nil => (entryName, None)
      case ext :: "tar" :: name =>
        (name.reverse.mkString("."), Option(s"tar.$ext"))
      case ext :: name => (name.reverse.mkString("."), Option(ext))
      case Nil => (entryName, None)
    }
  }
}

object FileListToZipList {
  def apply(): FileListToZipList = {
    val fetchBytes: Path => IO[Array[Byte]] =
      Files[IO].readAll(_).compile.to(Array)

    new FileListToZipList(fetchBytes)
  }
}

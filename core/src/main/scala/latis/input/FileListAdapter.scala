package latis.input

import java.net.URI

import scala.util.matching.Regex

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import fs2.io.file._

import latis.data.Data
import latis.data.RangeData
import latis.data.Sample
import latis.input.FileListAdapter.FileInfo
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.util.ConfigLike
import latis.util.Identifier.IdentifierStringContext
import latis.util.LatisException
import latis.util.NetUtils

/**
 * An adapter for creating datasets from directory listings.
 *
 * This adapter produces datasets of the form `(s,,1,,, ..., s,,N,,) →
 * uri` or `(s,,1,,, ..., s,,N,,) → (uri, size)` given a sequence of
 * regular expressions with a total of `N` capture groups between
 * them. Capture groups can appear in any of the regular expressions.
 *
 * The sequence of regular expressions selects paths from the root of
 * a directory tree (specified by the URI given to [[getData]]) to
 * files. Paths that do not match are not traversed. A directory tree
 * with files at depth `N` requires a sequence of `N` regular
 * expressions.
 *
 * The scalars `s,,1,,, ..., s,,N,,` are mapped to capture groups in
 * the given regular expression using the `columns` configuration key.
 * The scalars must be able to parse the string returned by the
 * associated capture group. If the `columns` configuration key is
 * unspecified, a direct mapping (first scalar is mapped to first
 * capture group, and so on) is assumed.
 *
 * The `columns` configuration key is a string of zero-indexed indices
 * corresponding to capture groups separated by semicolons (`;`). The
 * `N`th index specifies the capture group to be mapped to the scalar
 * `s,,N,,`. Multiple capture groups can be mapped to a single scalar
 * by separating their indices with a comma (`,`). These groups are
 * joined with a space before being mapped to the corresponding
 * scalar. For example, `1;0;2` will map the second capture group to
 * the first scalar, the first capture group to the second scalar, and
 * the third capture group to the third scalar. `2;0,1` will map the
 * third capture group to the first scalar, and map the first and
 * second capture groups joined with a space to the second scalar.
 *
 * The scalar `uri` will be a file URI to a file contained within the
 * directory given in [[getData]] or its subdirectories with a path
 * matching the regular expression. The ID must be `uri` and the type
 * must be `text`. If the `baseDir` configuration key is set to a file
 * URI, this URI will be relative to `baseDir`. This scalar is
 * expected to be present in the model.
 *
 * The scalar `size` will be the size of the corresponding file in
 * bytes. The ID must be `size` and the type must be `long`. This
 * scalar may be omitted from the model, in which case the size of
 * each file will not be computed.
 */
class FileListAdapter(
  model: DataType,
  config: FileListAdapter.Config
) extends StreamingAdapter[FileInfo] {

  override def recordStream(uri: URI): Stream[IO, FileInfo] =
    for {
      root  <- Stream.fromEither[IO](NetUtils.getFilePath(uri))
      files <- listFiles(root)
    } yield files

  override def parseRecord(info: FileInfo): Option[Sample] =
    for {
      (ds, rs) <- model match {
        case Function(d, r) => (d.getScalars, r.getScalars).some
        case _              => None
      }
      dv      = extractValues(info.path)
      _      <- (ds.length == dv.length).guard[Option]
      domain <- ds.zip(dv).traverse(p => p._1.parseValue(p._2)).toOption
      range  <- makeRange(rs, info.path, info.size).toOption
    } yield Sample(domain, range)

  /**
   * Produces a Stream of paths, lexicographically ordered and
   * optionally with the corresponding file size, to files contained
   * within `path` and its subdirectories.
   *
   * The size is only computed if there is a variable named "size" in
   * the model.
   */
  private def listFiles(path: Path): Stream[IO, FileInfo] = {
    val files: Stream[IO, Path] = smartWalk(path, config.pattern)

    // Get the size if there is a variable named "size."
    model match {
      case Function(_, rs) if rs.findVariable(id"size").isDefined =>
        files.evalMap { p =>
          Files[IO].size(p).map(s => FileInfo(p, s.some))
        }
      case _ => files.map(FileInfo(_, None))
    }
  }

  /**
   * Applies the regular expression to a file path, ordering and
   * joining the matches as specified by `config.columns`.
   */
  private def extractValues(path: Path): List[String] = {
    val groups: List[String] = {
      val segments = {
        // This is a hack to get around the fact that we can't access
        // the root from parseRecord, which is ultimately what calls
        // this method. Otherwise we'd just use the root and figure
        // out the relative path.
        val nSegmentsToKeep = config.pattern.length
        path.names.map(_.toString).takeRight(nSegmentsToKeep)
      }

      config.pattern.toList.zip(segments).flatMap { case (regex, segment) =>
        regex.findFirstMatchIn(segment).map(_.subgroups).getOrElse(List.empty)
      }
    }

    config.columns.map { ci =>
      if (groups.length > ci.flatten.max) {
        ci.map(_.map(groups(_)).mkString(" "))
      } else List.empty
    }.getOrElse(groups)
  }

  /**
   * Constructs the appropriate RangeData, taking into account whether
   * the size is required and whether the file URIs should be relative
   * to a given base directory.
   */
  private def makeRange(
    range: List[Scalar],
    path: Path,
    size: Option[Long]
  ): Either[LatisException, RangeData] = {
    val uri: URI = {
      val fileUri = path.toNioPath.toUri
      val baseUri = config.baseDir.map(_.toNioPath.toUri)
      baseUri.map(_.relativize(fileUri)).getOrElse(fileUri)
    }

    range match {
      case (u: Scalar) :: Nil if u.id == id"uri" =>
        u.parseValue(uri.toString).map(RangeData(_))
      case (u: Scalar) :: (s: Scalar) :: Nil
          if u.id == id"uri" && s.id == id"size" => for {
            uriDatum  <- u.parseValue(uri.toString)
            sizeLong  <- size.toRight {
              // This shouldn't happen. If we have the size scalar in
              // the model, we should have gotten the size in
              // listFiles, so this variable should be defined.
              LatisException("Missing size")
            }
            sizeDatum <- Data.fromValue(sizeLong)
          } yield RangeData(uriDatum, sizeDatum)
      case _ => LatisException("Unsupported range").asLeft
    }
  }

  /**
   * Returns a lexicographically ordered stream of paths to files
   * selected by `pattern`.
   */
  private def smartWalk(
    path: Path,
    pattern: NonEmptyList[Regex]
  ): Stream[IO, Path] = {
    def go(path: Path, pattern: List[Regex]): Stream[IO, Path] =
      pattern match {
        case head :: Nil =>
          // We are looking for files that match.
          val matchingFiles = Files[IO]
            .list(path)
            .evalFilter(Files[IO].isRegularFile)
            .filter(path => head.matches(path.fileName.toString))
            .compile
            .toVector

          Stream.evals(matchingFiles.map(_.sortBy(_.toString)))
        case head :: rest =>
          // We are looking for directories that match.
          val matchingDirs = Files[IO]
            .list(path)
            .evalFilter(Files[IO].isDirectory)
            .filter(path => head.matches(path.fileName.toString))
            .compile
            .toVector

          Stream
            .evals(matchingDirs.map(_.sortBy(_.toString)))
            .flatMap(go(_, rest))
        case Nil => Stream.empty
      }

    go(path, pattern.toList)
  }
}

object FileListAdapter extends AdapterFactory {
  /**
   * Creates a [[FileListAdapter]].
   *
   * @throws LatisException if required configuration keys are unset
   * or if any configuration values are malformed
   */
  def apply(model: DataType, config: AdapterConfig): FileListAdapter = {
    val c = FileListAdapter.Config.fromConfigLike(config).fold(throw _, identity)
    new FileListAdapter(model, c)
  }

  /**
   * Information required from each file in the directory listing.
   *
   * This type represents a pair of the file path and the file size.
   * The file size is defined only if the file size was requested.
   */
  final case class FileInfo(path: Path, size: Option[Long])

  /**
   * Type-safe configuration for the file list adapter.
   *
   * @param pattern regular expression to apply to each path
   * @param columns nested list of column indices, the Nth inner list
   * contains the indices of capture groups to be mapped to the Nth
   * scalar in the domain
   * @param baseDir path to relativize file paths against
   */
  final case class Config(
    pattern: NonEmptyList[Regex],
    columns: Option[List[List[Int]]],
    baseDir: Option[Path]
  )

  object Config {
    /**
     * Creates a type-safe configuration from a stringly typed map.
     *
     * Handles the following configuration keys:
     *
     *  - `pattern`: A required string containing a sequence of
     *    regular expressions delimited by forward slashes. A
     *    directory tree with files at depth `M` must have `M` regular
     *    expressions. The entire string must have a total of `N`
     *    capture groups for `N` scalars in the domain.
     *  - `columns`: An optional string of zero-based indices
     *    separated by semicolons or commas. See [[FileListAdapter]]
     *    for a more detailed description.
     *  - `baseDir`: An optional file URI that the URIs of files
     *    returned by this adapter will be relativized against.
     */
    def fromConfigLike(cl: ConfigLike): Either[LatisException, Config] = for {
      pattern <- cl.get("pattern").toRight {
        LatisException("Adapter requires a pattern definition")
      }.flatMap(parsePattern)
      columns <- cl.get("columns").traverse(parseColumns)
      baseDir <- cl.get("baseDir").traverse {
        NetUtils.parseUri(_).flatMap(NetUtils.getFilePath)
      }
    } yield Config(pattern, columns, baseDir)

    private def parseColumns(cols: String): Either[LatisException, List[List[Int]]] =
      Either.catchOnly[NumberFormatException] {
        cols.split(";").map(_.split(",").map(_.toInt).toList).toList
      }.leftMap {
        new LatisException("Column specification must contain only numbers", _)
      }

    private def parsePattern(
      str: String
    ): Either[LatisException, NonEmptyList[Regex]] =
      NonEmptyList.fromList(str.split("/").toList.map(_.r))
        .toRight(LatisException(s"Failed to construct pattern from: $str"))
  }
}

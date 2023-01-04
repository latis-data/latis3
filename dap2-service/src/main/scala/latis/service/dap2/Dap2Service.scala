package latis.service.dap2

import java.net.URLDecoder

import cats.effect._
import cats.syntax.all._
import fs2.Stream
import fs2.io.file.Files
import fs2.io.file.{Path => FPath}
import fs2.text
import org.http4s.Header.Raw
import org.http4s.Headers
import org.http4s.HttpRoutes
import org.http4s.MediaType
import org.http4s.Response
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.Accept
import org.http4s.scalatags.scalatagsEncoder
import org.typelevel.ci._

import latis.catalog.Catalog
import latis.dataset.Dataset
import latis.ops
import latis.ops.UnaryOperation
import latis.output._
import latis.server.ServiceInterface
import latis.service.dap2.error._
import latis.util.Identifier
import latis.util.dap2.parser.ConstraintParser
import latis.util.dap2.parser.ast
import latis.util.dap2.parser.ast.ConstraintExpression

/**
 * A service interface implementing the DAP 2 specification.
 */
class Dap2Service(catalog: Catalog) extends ServiceInterface(catalog) with Http4sDsl[IO] {

  override def routes: HttpRoutes[IO] =
    HttpRoutes.of {
      case req @ GET -> path => // path relative to "dap2", starting with "/"
        handleGetRequest(path, req.queryString, req.headers)
    }

  /** Handles GET requests for a Catalog or Dataset. */
  private def handleGetRequest(path: Path, query: String, headers: Headers): IO[Response[IO]] = {
    if (path.isEmpty) catalogResponse(catalog, headers)
    else parsePath(path) match {
      case (Some(id), ext) => catalog.findDataset(id).flatMap {
        case Some(ds) =>
          // Dataset path should not end with slash
          if (path.endsWithSlash) NotFound(s"Resource not found: $path")
          else datasetResponse(ds, ext, query)
        case None     => catalog.findCatalog(id) match {
          case Some(cat) => catalogResponse(cat, headers)
          case None      => NotFound(s"Resource not found: $path")
        }
      }
      case (None, _) => NotFound(s"Resource not found: $path")
    }
  }

  /**
   * Extracts the fully qualified catalog or dataset identifier
   * and optional output extension from the request URL path.
   *
   * Each segment of the path will be interpreted as a nested catalog with the
   * last segment representing a catalog or a dataset with an optional file extension.
   * This assumes that the path is not empty, a case that should already be handled.
   *
   * The returned identifier is optional to account for an invalid path.
   */
  private def parsePath(path: Path): (Option[Identifier], Option[String]) = {
    val segs = path.segments.map(_.toString)
    val (lastSeg, ext) =
      if (segs.last.endsWith(".")) (None, None)
      else segs.last.split('.').toList match {
        case id :: ext :: Nil => (Some(id), Some(ext))
        case id:: Nil         => (Some(id), None) //"foo." will match here keeping "foo"
        case _                => (None, None) //invalid unqualified id
      }
    val id = lastSeg.flatMap { last =>
      val ids = segs.dropRight(1) :+ last
      if (ids.exists(s => s.isEmpty || s.contains('.'))) None //invalid unqualified id
      else Identifier.fromString(ids.mkString("."))
    }
    (id, ext)
  }

  /**
   * Provides a response for a Catalog request.
   *
   * This determines whether the JSON or HTML response satisfies the Accept header first.
   * If both match or there is no Accept header, JSON will be used over HTML. If none match,
   * this will respond with a NotAcceptable (406).
   */
  private def catalogResponse(catalog: Catalog, headers: Headers): IO[Response[IO]] =
    headers.get[Accept] match {
      case Some(accept) =>
        accept.values.map(_.mediaRange).map { mr =>
          if (MediaType.application.json.satisfies(mr)) JsonCatalogEncoder.encode(catalog).flatMap(Ok(_)).some
          else if (MediaType.text.html.satisfies(mr)) HtmlCatalogEncoder.encode(catalog).flatMap(Ok(_)).some
          else None
        }.toList.unite.headOption //take first nonNone response
         .getOrElse(NotAcceptable("A catalogs is available only as JSON or HTML."))
      case None => JsonCatalogEncoder.encode(catalog).flatMap(Ok(_))
    }

  /** Provides a response for a Dataset request. */
  private def datasetResponse(
    dataset: Dataset,
    ext: Option[String],
    query: String
  ): IO[Response[IO]] = {
    (for {
      ops      <- IO.fromEither(getOperations(query))
      ds        = dataset.withOperations(ops)
      encoding <- IO.fromEither(encodeDataset(ds, ext))
      bytes     = encoding._1
      headers   = encoding._2
      response <- Ok(bytes).map(_.withHeaders(headers))
    } yield response).recoverWith {
      case err: Dap2Error => handleDap2Error(err)
    }
  }

  private def getOperations(query: String): Either[Dap2Error, List[UnaryOperation]] = {
    val ce = URLDecoder.decode(query, "UTF-8")

    ConstraintParser.parse(ce)
      .leftMap(ParseFailure)
      .flatMap { cexprs: ConstraintExpression =>
        cexprs.exprs.traverse {
          case ast.Projection(vs)      => Right(ops.Projection(vs:_*))
          case ast.Selection(n, op, v) => Right(ops.Selection(n, op, stripQuotes(v)))
          // Delegate to Operation factory
          case ast.Operation(name, args) =>
            UnaryOperation.makeOperation(name, args.map(stripQuotes))
              .leftMap(le => InvalidOperation(le.message))
        }
      }
  }

  //TODO: StringUtil?
  private def stripQuotes(str: String): String =
    str.stripPrefix("\"").stripSuffix("\"")

  private def encodeDataset(
    ds: Dataset,
    ext: Option[String]
  ): Either[Dap2Error, (Stream[IO, Byte], Headers)] = ext.getOrElse("meta") match {
    case "asc"   => new TextEncoder().encode(ds).through(text.utf8.encode).asRight
      .map((_,Headers(Raw(ci"Content-Type", "text/plain"))))
    case "bin"   => new BinaryEncoder().encode(ds).asRight
      .map((_, Headers(Raw(ci"Content-Type", "application/octet-stream"))))
    case "csv"   => CsvEncoder.withColumnName.encode(ds).through(text.utf8.encode).asRight
      .map((_, Headers(Raw(ci"Content-Type", "text/csv"))))
    case "das"   => new DasEncoder().encode(ds).through(text.utf8.encode).asRight
      .map((_, Headers(Raw(ci"Content-Type", "text/plain"), Raw(ci"Content-Description", "dods-das"))))
    case "dds"   => new DdsEncoder().encode(ds).through(text.utf8.encode).asRight
      .map((_, Headers(Raw(ci"Content-Type", "text/plain"), Raw(ci"Content-Description", "dods-dds"))))
    case "dods" => new DataDdsEncoder().encode(ds).asRight
      .map((_, Headers(Raw(ci"Content-Type", "application/octet-stream"), Raw(ci"Content-Description", "dods-data"))))
    case "jsonl" => new JsonEncoder().encode(ds).map(_.noSpaces).intersperse("\n").through(text.utf8.encode).asRight
      .map((_, Headers(Raw(ci"Content-Type", "application/jsonl"))))
    case "meta"  => new MetadataEncoder().encode(ds).map(_.noSpaces).through(text.utf8.encode).asRight
      .map((_, Headers(Raw(ci"Content-Type", "application/json"))))
    case "nc"    =>
      (for {
        tmpFile <- Stream.resource(Files[IO].tempFile)
        file    <- new NetcdfEncoder(tmpFile.toNioPath.toFile()).encode(ds)
        bytes   <- Files[IO].readAll(FPath.fromNioPath(file.toPath()))
      } yield bytes).asRight.map((_, Headers(Raw(ci"Content-Type", "application/x-netcdf"))))
    case "txt"   => CsvEncoder().encode(ds).through(text.utf8.encode).asRight
      .map((_, Headers(Raw(ci"Content-Type", "text/plain"))))
    case "zip"   => new ZipEncoder().encode(ds.withOperation(ops.FileListToZipList())).asRight
      .map((_, Headers(Raw(ci"Content-Type", "application/zip"))))
    case _       => UnknownExtension(s"Unknown extension: $ext").asLeft
  }

  private def handleDap2Error(err: Dap2Error): IO[Response[IO]] =
    err match {
      case DatasetResolutionFailure(msg) => NotFound(msg)
      case ParseFailure(msg)             => BadRequest(msg)
      case UnknownExtension(msg)         => BadRequest(msg)
      case UnknownOperation(msg)         => BadRequest(msg)
      case InvalidOperation(msg)         => BadRequest(msg)
    }
}

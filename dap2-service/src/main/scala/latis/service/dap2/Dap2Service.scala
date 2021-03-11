package latis.service.dap2

import java.net.URLDecoder
import java.nio.file.Paths

import scala.util.Properties

import cats.effect._
import cats.syntax.all._
import fs2.Stream
import fs2.io
import fs2.text
import org.http4s.headers.`Content-Type`
import org.http4s.HttpRoutes
import org.http4s.MediaType
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import latis.dataset.Dataset
import latis.input.DatasetResolver
import latis.ops
import latis.ops.UnaryOperation
import latis.output._
import latis.server.ServiceInterface
import latis.service.dap2.error._
import latis.util.Identifier
import latis.util.StreamUtils
import latis.util.dap2.parser.ConstraintParser
import latis.util.dap2.parser.ast
import latis.util.dap2.parser.ast.ConstraintExpression

/**
 * A service interface implementing the DAP 2 specification.
 */
class Dap2Service extends ServiceInterface with Http4sDsl[IO] {

  override def routes: HttpRoutes[IO] =
    HttpRoutes.of {
      case req @ GET -> Root / id ~ ext =>
        (for {
          ident    <- IO.fromOption(Identifier.fromString(id))(ParseFailure(s"'$id' is not a valid identifier"))
          dataset  <- IO.fromEither(getDataset(ident))
          ops      <- IO.fromEither(getOperations(req.queryString))
          result    = ops.foldLeft(dataset)((ds, op) => ds.withOperation(op))
          bytes    <- IO.fromEither(encode(ext, result))
          response <- Ok(bytes._1).map(_.withContentType(bytes._2))
        } yield response).handleErrorWith {
          case err: Dap2Error => handleDap2Error(err)
          case _              => InternalServerError()
        }
    }

  private def getDataset(id: Identifier): Either[Dap2Error, Dataset] =
    Either.catchNonFatal {
      DatasetResolver.getDataset(id)
    }.leftMap(_ => DatasetResolutionFailure(s"Failed to resolve dataset: $id"))

  private def getOperations(query: String): Either[Dap2Error, List[UnaryOperation]] = {
    val ce = URLDecoder.decode(query, "UTF-8")

    ConstraintParser.parse(ce)
      .leftMap(ParseFailure(_))
      .flatMap { cexprs: ConstraintExpression =>
        cexprs.exprs.traverse {
          case ast.Projection(vs)      => Right(ops.Projection(vs:_*))
          case ast.Selection(n, op, v) => Right(ops.Selection(n, op, stripQuotes(v)))
          case ast.Operation("rename", oldName :: newName :: Nil) => for {
              oldName <- Identifier.fromString(oldName).toRight(
                InvalidOperation(s"Invalid variable name $oldName")
              )
              newName <- Identifier.fromString(newName).toRight(
                InvalidOperation(s"Invalid variable name $newName")
              )
            } yield ops.Rename(oldName, newName)
          // TODO: Here we may need to dynamically construct an
          // instance of an operation based on the query string and
          // server/interface configuration.
          case ast.Operation(n, _)  => Left(UnknownOperation(s"Unknown operation: $n"))
        }
      }
  }

  private def stripQuotes(str: String): String =
    str.stripPrefix("\"").stripSuffix("\"")

  private def encode(ext: String, ds: Dataset): Either[Dap2Error, (Stream[IO, Byte], `Content-Type`)] = ext match {
    case ""     => encode("html", ds)
    case "bin"  => new BinaryEncoder().encode(ds).flatMap {
      bits => Stream.emits(bits.toByteArray)
    }.asRight.map((_, `Content-Type`(MediaType.application.`octet-stream`)))
    case "csv"  => CsvEncoder.withColumnName.encode(ds).through(text.utf8Encode).asRight
      .map((_, `Content-Type`(MediaType.text.csv)))
    case "jsonl" => new JsonEncoder().encode(ds).map(_.noSpaces).intersperse("\n").through(text.utf8Encode).asRight
      .map((_, `Content-Type`(MediaType.unsafeParse("application/jsonl")))) //TODO: verify safety of unsafeParse. Could throw ParseFailure.
    case "nc"   =>
      implicit val cs = StreamUtils.contextShift
      (for {
        tmpFile <- io.file.tempFileStream[IO](
          StreamUtils.blocker,
          Paths.get(Properties.tmpDir)
        )
        file    <- new NetcdfEncoder(tmpFile.toFile()).encode(ds)
        bytes   <- io.file.readAll[IO](file.toPath(), StreamUtils.blocker, 4096)
      } yield bytes).asRight.map((_, `Content-Type`(MediaType.application.`x-netcdf`)))
    case "txt"  => new TextEncoder().encode(ds).through(text.utf8Encode).asRight
      .map((_,`Content-Type`(MediaType.text.plain)))
    case "meta" => new MetadataEncoder().encode(ds).map(_.noSpaces).through(text.utf8Encode).asRight
      .map((_,`Content-Type`(MediaType.application.json)))
    case _      => UnknownExtension(s"Unknown extension: $ext").asLeft
  }

  private def handleDap2Error(err: Dap2Error): IO[Response[IO]] =
    err match {
      case DatasetResolutionFailure(msg) => NotFound(msg)
      case ParseFailure(msg)             => BadRequest(msg)
      case UnknownExtension(msg)         => NotFound(msg)
      case UnknownOperation(msg)         => BadRequest(msg)
      case InvalidOperation(msg)         => BadRequest(msg)
    }
}

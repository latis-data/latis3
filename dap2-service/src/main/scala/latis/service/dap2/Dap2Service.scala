package latis.service.dap2

import java.net.URLDecoder

import scala.util.Try

import cats.effect._
import cats.implicits._
import org.http4s.HttpRoutes
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import latis.input.FdmlDatasetResolver
import latis.dataset.Dataset
import latis.ops
import latis.ops.UnaryOperation
import latis.output.Encoder
import latis.output.TextEncoder
import latis.server.ServiceInterface
import latis.service.dap2.error._
import latis.util.dap2.parser.ConstraintParser
import latis.util.dap2.parser.ast

/**
 * A service interface implementing the DAP 2 specification.
 */
class Dap2Service(resolver: FdmlDatasetResolver) extends ServiceInterface(resolver) with Http4sDsl[IO] {

  override def routes: HttpRoutes[IO] =
    HttpRoutes.of {
      case req @ GET -> Root / id ~ ext =>
        (for {
          dataset  <- IO.fromEither(getDataset(id))
          ops      <- IO.fromEither(getOperations(req.queryString))
          result    = ops.foldLeft(dataset)((ds, op) => ds.withOperation(op))
          encoder  <- IO.fromEither(getEncoder(ext))
          response <- Ok(encoder.encode(result))
        } yield response).handleErrorWith {
          case err: Dap2Error => handleDap2Error(err)
          case _              => InternalServerError()
        }
    }

  private def getDataset(id: String): Either[Dap2Error, Dataset] =
    Try(resolver.getDataset(id).get)
      .toEither
      .leftMap(_ => DatasetResolutionFailure(s"Failed to resolve dataset: $id"))

  private def getOperations(query: String): Either[Dap2Error, List[UnaryOperation]] = {
    val ce = URLDecoder.decode(query, "UTF-8")

    ConstraintParser.parse(ce)
      .leftMap(ParseFailure(_))
      .flatMap { cexprs: ast.ConstraintExpression =>
        cexprs.exprs.traverse {
          case ast.Projection(vs)      => Right(ops.Projection(vs:_*))
          case ast.Selection(n, op, v) => Right(ops.Selection(n, ast.prettyOp(op), v))
          // TODO: Here we may need to dynamically construct an
          // instance of an operation based on the query string and
          // server/interface configuration.
          case ast.Operation(n, _)  => Left(UnknownOperation(s"Unknown operation: $n"))
        }
      }
  }

  private def getEncoder(ext: String): Either[Dap2Error, Encoder[IO, String]] =
    ext match {
      case ""    => getEncoder("html")
      case "txt" => Right(new TextEncoder)
      // TODO: Here we may need to dynamically construct an instance
      // of an encoder based on the extension and server/interface
      // configuration.
      case _     => Left(UnknownExtension(s"Unknown extension: $ext"))
    }

  private def handleDap2Error(err: Dap2Error): IO[Response[IO]] =
    err match {
      case DatasetResolutionFailure(msg) => NotFound(msg)
      case ParseFailure(msg)             => BadRequest(msg)
      case UnknownExtension(msg)         => NotFound(msg)
      case UnknownOperation(msg)         => BadRequest(msg)
    }
}

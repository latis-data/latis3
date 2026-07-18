package latis.service.sql

import cats.data.OptionT
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import fs2.text.utf8
import org.http4s.HttpRoutes
import org.http4s.MediaType
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`

import latis.catalog.Catalog
import latis.dataset.Dataset
import latis.ops.OperationRegistry
import latis.ops.Projection as LProjection
import latis.ops.Selection as LSelection
import latis.ops.UnaryOperation
import latis.output.TextEncoder
import latis.server.ServiceInterface
import latis.util.Identifier
import latis.util.dap2.parser.ast

import SqlServiceError.*
import parser.SqlParser

final class SqlService(catalog: Catalog)
  extends ServiceInterface(catalog, OperationRegistry.default) {

  private val dsl = new Http4sDsl[IO] {}
  import dsl.*

  override def routes: HttpRoutes[IO] =
    HttpRoutes.of {
      case req @ POST -> Root => (for {
        queryStr <- req.as[String]
        query    <- IO.fromEither(SqlParser.parse(queryStr))
        dataset  <- getDataset(query.dataset)
        ops       = toLatisOps(query)
        result    = dataset.withOperations(ops)
        encoded   = encodeDataset(result)
        result   <- Ok(encoded).map {
          _.withContentType(
            `Content-Type`(MediaType.text.plain)
          )
        }
      } yield result).recoverWith {
        case err: SqlServiceError => handleServiceErrors(err)
      }
    }

  private def encodeDataset(ds: Dataset): Stream[IO, Byte] =
    new TextEncoder().encode(ds).through(utf8.encode)

  private def getDataset(id: Identifier): IO[Dataset] =
    OptionT(catalog.findDataset(id)).cataF(
      IO.raiseError(DatasetResolutionFailure(s"Dataset not found: ${id.asString}")),
      _.pure[IO]
    )

  private def handleServiceErrors(err: SqlServiceError): IO[Response[IO]] =
    err match {
      case DatasetResolutionFailure(msg) => BadRequest(msg)
      case ParseError(msg)               => BadRequest(msg)
    }

  private def toLatisOps(q: Query): List[UnaryOperation] = {
    def toLatisProjection(p: List[Identifier]): Option[LProjection] =
      Option.when(p.nonEmpty)(LProjection(p*))

    def toLatisSelection(s: Selection): LSelection = {
      def toLatisOp(op: Selection.Op): ast.SelectionOp = op match {
        case Selection.Eq   => ast.Eq
        case Selection.Gt   => ast.Gt
        case Selection.GtEq => ast.GtEq
        case Selection.Lt   => ast.Lt
        case Selection.LtEq => ast.LtEq
      }

      LSelection(s.variable, toLatisOp(s.op), s.value)
    }

    val selection: List[UnaryOperation] = q.selection.map(toLatisSelection)
    val projection = toLatisProjection(q.projection)

    projection.fold(selection)(_ :: selection)
  }
}

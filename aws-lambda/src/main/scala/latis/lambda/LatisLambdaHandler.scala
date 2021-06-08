package latis.lambda

import java.net.URLDecoder

import scala.jdk.CollectionConverters._

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse
import org.http4s.MediaType
import org.typelevel.log4cats.slf4j.Slf4jLogger

import latis.catalog.Catalog
import latis.dataset.Dataset
import latis.lambda.error._
import latis.ops
import latis.ops.UnaryOperation
import latis.output.CsvEncoder
import latis.output.Encoder
import latis.output.JsonEncoder
import latis.output.MetadataEncoder
import latis.output.TextEncoder
import latis.util.Identifier
import latis.util.dap2.parser.ConstraintParser
import latis.util.dap2.parser.ast
import latis.util.dap2.parser.ast.ConstraintExpression

final class LatisLambdaHandler extends RequestHandler[APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse] {

  private val catalog: Catalog = Catalog.empty

  def handleRequest(
    req: APIGatewayV2HTTPEvent,
    ctx: Context
  ): APIGatewayV2HTTPResponse = {
    val params: Map[String, String] = req.getPathParameters().asScala.toMap
    val query: String = Option(req.getRawQueryString()).getOrElse("")

    Slf4jLogger.create[IO].flatMap { logger =>
      (for {
        id  <- IO.fromEither(parseDatasetId(params))
        hmm <- IO.fromEither(parseExtension(params))
        enc  = hmm._1
        typ  = hmm._2
        ops <- IO.fromEither(parseQuery(query))
        ds  <- getDataset(id)
        res  = ops.foldLeft(ds)((ds, op) => ds.withOperation(op))
        bdy <- enc.encode(res).compile.string
      } yield ok(bdy, typ)).handleErrorWith {
        case err: LatisLambdaError => handleError(err).pure[IO]
        case err =>
          logger.error(err)("Unhandled exception") *>
          internalServerError.pure[IO]
      }
    }.unsafeRunSync()
  }

  // Response constructors

  private def badRequest(body: String): APIGatewayV2HTTPResponse =
    APIGatewayV2HTTPResponse
      .builder()
      .withStatusCode(400)
      .withBody(body)
      .build()

  private def internalServerError: APIGatewayV2HTTPResponse =
    APIGatewayV2HTTPResponse
      .builder()
      .withStatusCode(500)
      .build()

  private def notFound(body: String): APIGatewayV2HTTPResponse =
    APIGatewayV2HTTPResponse
      .builder()
      .withStatusCode(404)
      .withBody(body)
      .build()

  private def ok(body: String, typ: MediaType): APIGatewayV2HTTPResponse =
    APIGatewayV2HTTPResponse
      .builder()
      .withStatusCode(200)
      .withHeaders(Map("Content-Type" -> typ.toString()).asJava)
      .withBody(body)
      .build()

  // Helper methods

  private def getDataset(id: Identifier): IO[Dataset] =
    catalog.findDataset(id).flatMap {
      case None => IO.raiseError {
        DatasetResolutionFailure(s"Failed to resolve dataset: ${id.asString}")
      }
      case Some(ds) => ds.pure[IO]
    }

  private def handleError(err: LatisLambdaError): APIGatewayV2HTTPResponse =
    err match {
      case DatasetResolutionFailure(msg) => notFound(msg)
      case ParseFailure(msg)             => badRequest(msg)
      case UnknownExtension(msg)         => notFound(msg)
      case InvalidOperation(msg)         => badRequest(msg)
    }

  private def parseDatasetId(
    params: Map[String, String]
  ): Either[ParseFailure, Identifier] = for {
    idStr <- Either.fromOption(
      params.get("id"),
      ParseFailure("No dataset id provided")
    )
    id    <- Either.fromOption(
      Identifier.fromString(idStr),
      ParseFailure(s"'$idStr' is not a valid identifier")
    )
  } yield id

  private def parseExtension(
    params: Map[String, String]
  ): Either[LatisLambdaError, (Encoder[IO, String], MediaType)] =
    params.get("ext") match {
      case Some("csv")   =>
        (CsvEncoder.withColumnName, MediaType.text.csv).asRight
      case Some("jsonl") =>
        (new JsonEncoder().map(_.noSpaces ++ "\n"), MediaType.unsafeParse("application/jsonl")).asRight
      case Some("meta")  =>
        (new MetadataEncoder().map(_.noSpaces), MediaType.application.json).asRight
      case Some("txt")   => (new TextEncoder(), MediaType.text.plain).asRight
      case Some(ext)     => UnknownExtension(s"Unknown extension: $ext").asLeft
      case None          => ParseFailure("No extension provided").asLeft
    }

  private def parseQuery(
    str: String
  ): Either[LatisLambdaError, List[UnaryOperation]] = {
    val ce = URLDecoder.decode(str, "UTF-8")

    def stripQuotes(str: String): String =
      str.stripPrefix("\"").stripSuffix("\"")

    ConstraintParser.parse(ce)
      .leftMap(ParseFailure(_))
      .flatMap { cexprs: ConstraintExpression =>
        cexprs.exprs.traverse {
          case ast.Projection(vs) =>
            Right(ops.Projection(vs:_*))
          case ast.Selection(n, op, v) =>
            Right(ops.Selection(n, op, stripQuotes(v)))
          // Delegate to Operation factory
          case ast.Operation(name, args) =>
            UnaryOperation.makeOperation(name, args.map(stripQuotes(_)))
              .leftMap(le => InvalidOperation(le.message))
        }
      }
  }
}

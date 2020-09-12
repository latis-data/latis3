package latis.input.fdml

import java.net.URI

import cats.syntax.all._

import latis.dataset.AdaptedDataset
import latis.dataset.Dataset
import latis.input.Adapter
import latis.metadata.Metadata
import latis.model._
import latis.ops
import latis.ops.UnaryOperation
import latis.ops.parser.ast
import latis.util.LatisException
import latis.util.ReflectionUtils

/** Provides methods for creating datasets from FDML. */
object FdmlReader {

  /** Creates a dataset from a URI pointing to FDML. */
  def read(uri: URI, validate: Boolean = false): Dataset = (for {
    fdml    <- FdmlParser.parseUri(uri, validate)
    dataset <- read(fdml)
  } yield dataset).fold(throw _, identity)

  /** Creates a dataset from FDML. */
  def read(fdml: Fdml): Either[LatisException, Dataset] = for {
    model      <- makeFunction(fdml.model)
    adapter    <- makeAdapter(fdml.adapter, model)
    operations <- fdml.operations.traverse(makeOperation)
  } yield new AdaptedDataset(
    fdml.metadata,
    model,
    adapter,
    fdml.source.uri,
    operations
  )

  private def makeDataType(model: FModel): Either[LatisException, DataType] =
    model match {
      case f: FFunction => makeFunction(f)
      case t: FTuple    => makeTuple(t)
      case s: FScalar   => makeScalar(s)
    }

  private def makeFunction(function: FFunction): Either[LatisException, Function] =
    for {
      domain <- makeDataType(function.domain)
      range  <- makeDataType(function.range)
    } yield Function(Metadata(function.attributes), domain, range)

  private def makeTuple(tuple: FTuple): Either[LatisException, Tuple] =
    (tuple.fst :: tuple.snd :: tuple.rest).traverse(makeDataType).map {
      Tuple(Metadata(tuple.attributes), _)
    }

  private def makeScalar(scalar: FScalar): Either[LatisException, Scalar] =
    scalar.attributes.get("class") match {
      case None       => Scalar(scalar.metadata).asRight
      case Some(clss) => Either.catchNonFatal {
        ReflectionUtils.callMethodOnCompanionObject(
          clss,
          "apply",
          scalar.metadata
        ).asInstanceOf[Scalar]
      }.leftMap(LatisException(_))
    }

  private def makeAdapter(
    adapter: FAdapter,
    model: DataType
  ): Either[LatisException, Adapter] = Either.catchNonFatal {
    ReflectionUtils.callMethodOnCompanionObject(
      "latis.input.AdapterFactory",
      "makeAdapter",
      model,
      adapter.config
    ).asInstanceOf[Adapter]
  }.leftMap(LatisException(_))

  private def makeOperation(
    expression: ast.CExpr
  ): Either[LatisException, UnaryOperation] =
    expression match {
      case ast.Projection(vs) => Right(ops.Projection(vs: _*))
      case ast.Selection(n, op, v) =>
        Right(ops.Selection(n, ast.prettyOp(op), v))
      case ast.Operation(name, args) =>
        UnaryOperation.makeOperation(name, args)
    }
}

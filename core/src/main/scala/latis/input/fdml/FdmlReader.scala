package latis.input.fdml

import java.net.URI

import cats.syntax.all._

import latis.dataset.AdaptedDataset
import latis.dataset.Dataset
import latis.dataset.GranuleAppendDataset
import latis.input.Adapter
import latis.model._
import latis.ops
import latis.ops.UnaryOperation
import latis.util.LatisException
import latis.util.ReflectionUtils
import latis.util.dap2.parser.ast
import latis.util.Identifier

/** Provides methods for creating datasets from FDML. */
object FdmlReader {

  /** Creates a dataset from a URI pointing to FDML. */
  def read(uri: URI, validate: Boolean = false): Dataset = (for {
    fdml    <- FdmlParser.parseUri(uri, validate)
    dataset <- read(fdml)
  } yield dataset).fold(throw _, identity)

  /** Creates a dataset from FDML. */
  def read(fdml: Fdml): Either[LatisException, Dataset] = fdml match {
    case fdml: DatasetFdml       => readDatasetFdml(fdml)
    case fdml: GranuleAppendFdml => readGranuleAppendFdml(fdml)
  }

  private def readDatasetFdml(
    fdml: DatasetFdml
  ): Either[LatisException, Dataset] = for {
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

  private def readGranuleAppendFdml(
    fdml: GranuleAppendFdml
  ): Either[LatisException, Dataset] = for {
    sid        <- Either.fromOption(
      fdml.metadata.getProperty("id"),
      LatisException("Missing identifier")
    )
    id         <- Either.fromOption(
      Identifier.fromString(sid),
      LatisException(s"Invalid identifier: $sid")
    )
    granules   <- readDatasetFdml(fdml.source.fdml)
    model      <- makeFunction(fdml.model)
    adapter    <- makeAdapter(fdml.adapter, model)
    operations <- fdml.operations.traverse(makeOperation)
    dataset    <- GranuleAppendDataset.withAdapter(
                    id,
                    granules,
                    model,
                    adapter,
                    operations
                  )
  } yield dataset

  private def makeDataType(model: FModel): Either[LatisException, DataType] =
    model match {
      case f: FFunction => makeFunction(f)
      case t: FTuple    => makeTuple(t)
      case s: FScalar   => makeScalar(s)
    }

  private def getId(attributes: Map[String, String]): Either[LatisException, Option[Identifier]] =
    attributes.get("id").traverse { id =>
      Identifier.fromString(id).toRight(LatisException(s"Invalid id: $id"))
    }

  private def makeFunction(function: FFunction): Either[LatisException, Function] =
    for {
      domain <- makeDataType(function.domain)
      range  <- makeDataType(function.range)
      id     <- getId(function.attributes)
      f      <- Function.from(id, domain, range)
    } yield f

  private def makeTuple(tuple: FTuple): Either[LatisException, Tuple] =
    for {
      dts <- (tuple.fst :: tuple.snd :: tuple.rest).traverse(makeDataType)
      id  <- getId(tuple.attributes)
      tup <- Tuple.fromSeq(id, dts)
    } yield tup

  private def makeScalar(scalar: FScalar): Either[LatisException, Scalar] =
    Scalar.fromMetadata(scalar.metadata)

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
        Right(ops.Selection(n, op, v))
      case ast.Operation(name, args) =>
        UnaryOperation.makeOperation(name, args)
    }
}

package latis.input.fdml

import java.net.URI

import latis.input.AdapterConfig
import latis.metadata.Metadata
import latis.model.ValueType
import latis.util.Identifier
import latis.util.dap2.parser.ast.CExpr

/** Abstract representation of an FDML file. */
sealed trait Fdml

/** FDML file defining a dataset from a URI and adapter. */
final case class DatasetFdml(
  metadata: Metadata,
  source: UriSource,
  adapter: SingleAdapter,
  model: FFunction,
  operations: List[CExpr] = List.empty
) extends Fdml

/** FDML file defining a dataset from granules. */
final case class GranuleAppendFdml(
  metadata: Metadata,
  source: FdmlSource,
  adapter: NestedAdapter,
  model: FFunction,
  operations: List[CExpr] = List.empty
) extends Fdml

/**
 * Abstract representation of an adapter.
 *
 * Note that adapters require a class attribute that specifies the
 * class implementing the adapter. This requirement is expressed here
 * as a constructor argument. Other parts of the LaTiS ecosystem use
 * maps and fail at runtime. To safely interact with those parts, use
 * the `config` method rather than accessing `attributes` directly.
 *
 * @param clss fully qualified name of class implementing adapter
 * @param attributes additional metadata
 */
sealed abstract class FAdapter(
  clss: String,
  attributes: Map[String, String]
) {
  /**
   * Returns an [[AdapterConfig]] for this adapter.
   *
   * The implementing class, which is not necessarily included in
   * `attributes`, is included in this config.
   */
  def config: AdapterConfig =
    AdapterConfig((("class" -> clss) :: attributes.toList): _*)
}

/** A single, normal adapter. */
final case class SingleAdapter(
  clss: String,
  attributes: Map[String, String]
) extends FAdapter(clss, attributes)

/**
 * A nested adapter, where the outer adapter specifies how to join the
 * output of the inner adapter.
 */
final case class NestedAdapter(
  clss: String,
  attributes: Map[String, String],
  nested: SingleAdapter
) extends FAdapter(clss, attributes)

/** Abstract representation of a dataset source. */
sealed trait FSource

/** A source from a URI. */
final case class UriSource(uri: URI) extends FSource

/** A source from a granule list dataset. */
final case class FdmlSource(fdml: DatasetFdml) extends FSource

// Not using the FDM definition in latis.model because it can't
// enforce useful invariants (scalars have an ID and a type, tuples
// have at least two variables in them) and there'd need to be special
// logic to handle the difference between a scalar parsed from FDML
// and dynamically constructing the specific subclass of scalar
// specified by the "class" attribute.

sealed trait FModel

/**
 * Abstract representation of an FDM function.
 *
 * @param domain domain variable
 * @param range range variable
 * @param attributes additional metadata
 */
final case class FFunction(
  domain: FModel,
  range: FModel,
  attributes: Map[String, String]
) extends FModel

/**
 * Abstract representation of an FDM tuple.
 *
 * @param fst first variable in tuple
 * @param snd second variable in tuple
 * @param rest other variables in tuple
 * @param attributes additional metadata
 */
final case class FTuple(
  fst: FModel,
  snd: FModel,
  rest: List[FModel],
  attributes: Map[String, String]
) extends FModel

/**
 * Abstract representation of an FDM scalar.
 *
 * Note that scalars require an identifier and a type. This
 * requirement is expressed here as constructor arguments. Other parts
 * of the LaTiS ecosystem use maps and fail at runtime. To safely
 * interact with those parts, use the `metadata` method rather than
 * accessing `attributes` directly.
 *
 * @param id identifier for scalar
 * @param ty type of scalar
 * @param attributes additional metadata
 */
final case class FScalar(
  id: Identifier,
  ty: ValueType,
  attributes: Map[String, String]
) extends FModel {

  /**
   * Returns metadata for this scalar.
   *
   * The identifier and type, which are not necessarily included in
   * `attributes`, are included in this metadata.
   */
  def metadata: Metadata = Metadata(attributes) ++
    Metadata("id" -> id.asString, "type" -> ty.toString.toLowerCase)
}

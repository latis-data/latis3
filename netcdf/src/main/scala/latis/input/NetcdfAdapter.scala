package latis.input

import java.net.URI

import cats.effect.IO
import fs2._

import latis.data._
import latis.model._
import latis.ops.Head
import latis.ops.Operation
import latis.ops.Stride
import latis.util.ConfigLike

/**
 * Adapter for accessing data from the netcdf-java API. This should
 * support netCDF, HDF, and other file formats supported by the library.
 *
 * This delegates to the NetcdfWrapper helper class which encapsulates
 * a NetcdfFile and manages a latis.util.Section which specifies the subset of
 * the multi-dimensional variables to read. Some operations will be
 * handled by modifying the section being read. The section will be
 * chunked so this can stream large datasets with minimal impact on
 * memory. Use the chunkSize configuration property to specify the
 * nominal number of elements being read at one time. Only the first
 * dimension will be broken into chunks so the resulting chunk
 * size may be slightly larger than the requested chunkSize for multi-dimensional
 * datasets.
 *
 * Note that the netCDF data model does not capture the notion of
 * nested Functions. As a result, this adapter does not support
 * nested Functions. If a nested Function is desired, apply a
 * Curry operation.
 *
 * This adapter assumes a single Function. Domain variables must
 * map to a one-dimensional variable in the netCDF file. The number of
 * domain Scalars must match the number of dimensions. Each range
 * variable must have the same shape in the netCDF file, representing
 * the product set of the domain variables.
 *
 */
class NetcdfAdapter(
  model: DataType,
  config: NetcdfAdapter.Config = new NetcdfAdapter.Config()
) extends Adapter {
  //TODO: add support for more than 3 dimensions
  //TODO: memoize to avoid re-reading nested domain variables
  //TODO: support Int domain variables when there is no coordinate variable
  //      due to limited support for Index (e.g. partial index for Cartesian domains)
  //TODO: allow non-Functions so we can read scalar netCDF variables
  //TODO: support section spec in config for default subset?
  //      consider use of eval
  //      there are cases of domain vars with extra dimension, could work if length 1

  override def canHandleOperation(op: Operation): Boolean = op match {
    //TODO: handle domain variable selections with cadence
    //TODO: handle domain variable selections via binary search
    //TODO: handle projection, index complications
    //TODO: handle taking and dropping operations
    case _: Head   => true
    case _: Stride => true
    case _         => false
  }

  def getData(uri: URI, ops: Seq[Operation]): Data = {
    val samples: Stream[IO, Sample] = for {
      nc      <- Stream.resource(NetcdfWrapper.open(uri, model, config))
      section <- Stream.eval(nc.makeSection(ops))
      chunk   <- nc.chunkSection(section)
      samples <- nc.streamSamples(chunk)
    } yield samples

    StreamFunction(samples)
  }
}

object NetcdfAdapter extends AdapterFactory {

  /** Constructor used by the AdapterFactory. */
  def apply(model: DataType, config: AdapterConfig): NetcdfAdapter =
    new NetcdfAdapter(model, new NetcdfAdapter.Config(config.properties: _*))

  class Config(val properties: (String, String)*) extends ConfigLike {
    /** Nominal number of elements to include in a NetcdfFile read. */
    val chunkSize: Option[Int] = getInt("chunkSize")
  }
}

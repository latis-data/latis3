package latis.input

import java.net.URI

import scodec.Codec
import scodec.stream.StreamDecoder

import latis.data.{Data, Sample, SampledFunction, StreamFunction}
import latis.model._
import latis.ops.Operation
import latis.output.DataCodec

/**
 * Adapter for reading binary datasets.
 */
class BinaryAdapter(model: DataType, dataCodec: Scalar => Codec[Data] = DataCodec.defaultDataCodec) extends Adapter {

  /* Lazily retrieves the BinaryEncoder using the default DataCodec and creates a sampleCodec utilizing this adapter's model  */
  private lazy val sampleCodec: Codec[Sample] =
    latis.output.BinaryEncoder(dataCodec).sampleCodec(model)

  /* Creates a StreamDecoder from the sampleCodec */
  private lazy val sampleStreamDecoder: StreamDecoder[Sample] =
    StreamDecoder.many(sampleCodec)

  /**
   * Implements the Adapter interface to get data from a URI.
   * Data is returned as a SampledFunction.
   */
  def getData(uri: URI, ops: Seq[Operation] = Seq.empty): SampledFunction = {
    val sampleStream = StreamSource.getStream(uri).through(sampleStreamDecoder.toPipeByte)
    StreamFunction(sampleStream)
  }

}

//=============================================================================



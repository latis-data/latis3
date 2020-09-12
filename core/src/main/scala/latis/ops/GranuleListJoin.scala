package latis.ops

import java.net.URI

import cats.syntax.all._

import latis.data._
import latis.input._
import latis.model.DataType
import latis.util.LatisException
import latis.util.StreamUtils.unsafeStreamToSeq

/**
 * Given a "granule list" Dataset and an Adapter to parse each granule,
 * combine the data from each granule into a single Dataset.
 * The dataset must have a "uri" variable that is not in a nested Function.
 * This assumes that each granule has the same model and can be appended
 * to the previous granule.
 */
case class GranuleListJoin(
  granuleModel: DataType,
  granuleAdapter: Adapter
) extends UnaryOperation {
  //Note, we could stream but we want to be able to delegate to individual smart granules

  /**
   * Replaces the original model (of the granule list dataset)
   * with the model of the granules.
   */
  def applyToModel(model: DataType): Either[LatisException, DataType] =
    Right(granuleModel)

  /**
   * Applies the Adapter to each URI in the granule list dataset
   * to generate a SampledFunction for each and wrap them all
   * in a CompositeSampledFunction.
   */
  def applyToData(
    data: SampledFunction,
    model: DataType
  ): Either[LatisException, SampledFunction] = {

    // Get the position of the "uri" value within a Sample
    val pos: Either[LatisException, SamplePosition] = model.getPath("uri") match {
      case Some(path) if path.length == 1 => Right(path.head)
      case _ => Left(LatisException("URI can't be in a nested function."))
    }

    // Make function that can be mapped over the granule list data.
    // Extract the uri then apply that to the Adapter to get the data for that granule.
    val f: Sample => Either[LatisException, SampledFunction] = (sample: Sample) =>
      for {
        p <- pos
        v <- sample.getValue(p).toRight(LatisException("URI not found in sample"))
        uri <- Either
          .catchOnly[java.net.URISyntaxException](new URI(v.toString))
          .leftMap(LatisException(_))
      } yield granuleAdapter.getData(uri) //TODO: delegate ops?

    // Create SampledFunctions for each granule URI
    // and combine into a CompositeSampledFunction.
    // Note the unsafeStreamToSeq so we can get the Seq out of IO.
    //TODO: use StreamFunction to avoid unsafeStreamToSeq?
    //TODO: do we need a CompositeDataset?
    unsafeStreamToSeq(data.samples).toList.traverse(f).map(CompositeSampledFunction(_))
  }

}

object GranuleListJoin {

  def apply(granuleModel: DataType, config: AdapterConfig): GranuleListJoin =
    GranuleListJoin(granuleModel, AdapterFactory.makeAdapter(granuleModel, config))
}

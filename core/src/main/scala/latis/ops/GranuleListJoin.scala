package latis.ops

import java.net.URI

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream

import latis.data._
import latis.input._
import latis.model.DataType
import latis.util.LatisException

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
  //TODO: rename to GranuleListAppend?

  /**
   * Replaces the original model (of the granule list dataset)
   * with the model of the granules.
   */
  def applyToModel(model: DataType): Either[LatisException, DataType] =
    granuleModel.asRight

  /**
   * Applies the Adapter to each URI in the granule list dataset
   * to generate Data for each and appends them into a single Stream.
   */
  def applyToData(data: Data, model: DataType): Either[LatisException, Data] = {

    // Get the position of the "uri" value within a Sample
    val pos: Either[LatisException, SamplePosition] = model.getPath("uri") match {
      case Some(path) if path.length == 1 => path.head.asRight
      case _ => LatisException("uri not found.").asLeft
    }

    // Make function that can be mapped over the granule list data.
    // Extract the uri then apply that to the Adapter to get the data for that granule.
    val f: Sample => Either[LatisException, Data] = (sample: Sample) =>
      for {
        p <- pos
        v <- sample.getValue(p).toRight(LatisException("URI not found in sample"))
        uri <- Either
          .catchOnly[java.net.URISyntaxException](new URI(v.toString))
          .leftMap(LatisException(_))
      } yield granuleAdapter.getData(uri) //TODO: delegate ops?

    // Create Data for each granule URI and combine into one.
    val samples = data.asFunction.samples
      .map(f)
      .flatMap(Stream.fromEither[IO](_))
      .flatMap(_.asFunction.samples)
    StreamFunction(samples).asRight
  }

}

object GranuleListJoin {

  def apply(granuleModel: DataType, config: AdapterConfig): GranuleListJoin =
    GranuleListJoin(granuleModel, AdapterFactory.makeAdapter(granuleModel, config))
}

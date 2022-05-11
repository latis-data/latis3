package latis.output

import cats.effect.IO
import fs2.Stream
import io.circe.{Encoder => JEncoder}
import io.circe.Json
import io.circe.syntax._

import latis.dataset.Dataset

/**
 * An encoder that only encodes metadata.
 *
 * This encoder emits a single JSON object similar to the following:
 *
 * {{{
 * {
 *   "id": "...",
 *   "model": "...",
 *   ... other dataset metadata,
 *   "variables": [
 *     {
 *       "id": "...",
 *       ... other variable metadata
 *     },
 *     ...
 *   ]
 * }
 * }}}
 */
class MetadataEncoder extends Encoder[IO, Json] {

  override def encode(dataset: Dataset): Stream[IO, Json] =
    Stream.emit(MetadataEncoder.MetadataOnly(dataset).asJson)
}

object MetadataEncoder {
  private final case class MetadataOnly(ds: Dataset) extends AnyVal

  private implicit val mdEncoder: JEncoder[MetadataOnly] =
    new JEncoder[MetadataOnly] {
      def apply(md: MetadataOnly): Json = {
        val datasetMetadata = md.ds.metadata.properties.asJsonObject
        val variableMetadata = md.ds.model.getScalars.map(_.metadata.properties).asJson
        Json.fromJsonObject(
          datasetMetadata
            .add("model", md.ds.model.toString().asJson)
            .add("variable", variableMetadata)
        )
      }
    }
}

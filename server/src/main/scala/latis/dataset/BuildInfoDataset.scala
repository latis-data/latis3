package latis.dataset

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.util.BuildInfo
import latis.util.Identifier
import latis.util.Identifier.IdentifierStringContext

/**
 * Defines a Dataset that provides build information.
 *
 * This uses the BuildInfo that is derived by the sbt-buildinfo plugin.
 * This project provides a stub to support compilation. If the service
 * project does not use the sbt-buildinfo plugin, this dataset will use
 * default properties from the stub.
 */
object BuildInfoDataset {
  //TODO: deal with 0 or 1 property in the BuildInfo Map

  def apply(): Dataset = {
    val metadata = Metadata(id"build_info")

    val properties: List[(String, String)] = BuildInfo.toMap.toList.map {
      case (k: String, v) => (k, v.toString)
    }

    val scalars: List[Scalar] = properties.map { case (id, _) =>
      Scalar(Identifier.fromString(id).get, StringValueType)
    }

    val model = Tuple.fromSeq(scalars).fold(throw _, identity)

    val values: List[Data] = properties.map { case (_, value) =>
      Data.StringValue(value)
    }

    // Single zero-arity Sample
    val sample = Sample(
      DomainData(),
      RangeData(values)
    )

    val mf = SeqFunction(sample)

    new MemoizedDataset(metadata, model, mf)
  }
}

package latis.model

import latis.metadata.Metadata
import latis.util.Identifier

class Index private (md: Metadata, id: Identifier)
  extends Scalar(md, id, LongValueType)

object Index {

  def apply(id: Identifier): Index = {
    val md = Metadata("id" -> id.asString, "type" -> "long", "class" -> "latis.model.Index")
    new Index(md, id)
  }
}

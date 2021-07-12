package latis.model

import latis.metadata.Metadata
import latis.util.Identifier
import latis.util.Identifier.IdentifierStringContext

/**
 * Index is a Scalar that is used as a placeholder for a domain variable
 * that does not have a value represented in a Sample.
 */
class Index private (md: Metadata, id: Identifier)
  extends Scalar(md, id, LongValueType)

object Index {

  def apply(): Index = Index(id"_i")

  def apply(id: Identifier): Index = {
    val md = Metadata("id" -> id.asString, "type" -> "long", "class" -> "latis.model.Index")
    new Index(md, id)
  }
}

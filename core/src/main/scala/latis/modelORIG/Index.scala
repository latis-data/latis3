package latis.modelORIG

import latis.metadata.Metadata
import latis.util.Identifier

/**
 * Index is a Scalar that is used as a placeholder for a domain variable
 * that does not have a value represented in a Sample.
 */
class Index private (metadata: Metadata) extends Scalar(metadata) {
  //TODO: enforce that Index variables only appear in Function domains
  //TODO: make sure operations handle Index appropriately

  /** Override to preserve type */
  override def rename(id: Identifier): Index = Index(id)
}

object Index {

  /**
   * Constructs an Index variable with the given id.
   */
  def apply(id: Identifier): Index =
    new Index(Metadata(
      "id" -> id.asString,
      "type" -> "long" //Scalar needs value type
    ))
}

package latis.model

import latis.model.LongValueType
import latis.util.Identifier

class Index(id: Identifier) extends Scalar(id, LongValueType)

object Index {
  def apply(id: Identifier): Index = new Index(id)
}

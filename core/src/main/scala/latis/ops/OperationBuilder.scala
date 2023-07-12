package latis.ops

import latis.util.LatisException

trait OperationBuilder {

  def build(args: List[String]): Either[LatisException, UnaryOperation]
}

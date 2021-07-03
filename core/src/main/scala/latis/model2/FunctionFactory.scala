package latis.model2

import cats.syntax.all._

import latis.util.Identifier
import latis.util.LatisException

trait FunctionFactory {

  def from(id: Option[Identifier], domain: DataType, range: DataType): Either[LatisException, Function] = {
    //TODO: assert unique ids
    //TODO: assert no functions in domain, including within tuples
    //TODO: no Index in range
    new Function(id, domain, range).asRight
  }

  def from(id: Identifier, domain: DataType, range: DataType): Either[LatisException, Function] =
    from(Some(id), domain, range)

  def from(domain: DataType, range: DataType): Either[LatisException, Function] =
    from(None, domain, range)

}

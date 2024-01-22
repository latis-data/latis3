package latis.model

import cats.syntax.all.*

import latis.util.Identifier
import latis.util.LatisException

trait FunctionFactory {

  def from(id: Option[Identifier], domain: DataType, range: DataType): Either[LatisException, Function] = {
    //TODO: assert unique ids

    // No functions in domain, including within tuples
    val fInDomain: Boolean = domain match {
      case _: Function => true
      case t: Tuple    => t.flatElements.exists(_.isInstanceOf[Function])
      case _           => false
    }

    // No Index in range
    val indexInRange: Boolean = range match {
      case _: Index => true
      case t: Tuple => t.flatElements.exists(_.isInstanceOf[Index])
      case _        => false
    }

    if (fInDomain) LatisException("Function in Function domain not allowed").asLeft
    else if (indexInRange) LatisException("Index in Function range not allowed").asLeft
    else new Function(id, domain, range).asRight
  }

  def from(id: Identifier, domain: DataType, range: DataType): Either[LatisException, Function] =
    from(Some(id), domain, range)

  def from(domain: DataType, range: DataType): Either[LatisException, Function] =
    from(None, domain, range)

}

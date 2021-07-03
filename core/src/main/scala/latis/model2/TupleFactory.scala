package latis.model2

import cats.syntax.all._

import latis.util.Identifier
import latis.util.LatisException

trait TupleFactory {

  def fromElements(id: Option[Identifier], e1: DataType, e2: DataType, es: DataType*): Either[LatisException, Tuple] =
  //TODO: assert unique ids
    new Tuple(id, e1, e2, es: _*).asRight

  def fromElements(id: Identifier, e1: DataType, e2: DataType, es: DataType*): Either[LatisException, Tuple] =
    fromElements(Some(id), e1, e2, es: _*)

  def fromElements(e1: DataType, e2: DataType, es: DataType*): Either[LatisException, Tuple] =
    fromElements(None, e1, e2, es: _*)

  def fromSeq(id: Option[Identifier], dts: Seq[DataType]): Either[LatisException, Tuple] =
    dts match {
      case e1 :: e2 :: es => fromElements(id, e1, e2, es: _*)
      case _ => LatisException("Tuple must have at least two elements").asLeft
    }

  def fromSeq(id: Identifier, dts: Seq[DataType]): Either[LatisException, Tuple] = fromSeq(Some(id), dts)

  def fromSeq(dts: Seq[DataType]): Either[LatisException, Tuple] = fromSeq(None, dts)

}

package latis.model

import cats.syntax.all.*

import latis.util.Identifier
import latis.util.LatisException

trait TupleFactory {

  //TODO: deal with same id in nested anonymous tuples
  def fromElements(
    id: Option[Identifier],
    e1: DataType,
    e2: DataType,
    es: DataType*
  ): Either[LatisException, Tuple] = {
    def getId(e: DataType) = e match { //TODO: require ids?
      case s: Scalar   => Some(s.id)
      case t: Tuple    => t.id
      case f: Function => f.id
    }
    val ids = (e1 +: e2 +: es).flatMap(getId)
    if (ids.distinct.size == ids.size) new Tuple(id, e1, e2, es *).asRight
    else LatisException("Tuple elements must have distinct identifiers").asLeft
  }

  def fromElements(id: Identifier, e1: DataType, e2: DataType, es: DataType*): Either[LatisException, Tuple] =
    fromElements(Some(id), e1, e2, es *)

  def fromElements(e1: DataType, e2: DataType, es: DataType*): Either[LatisException, Tuple] =
    fromElements(None, e1, e2, es *)

  def fromSeq(id: Option[Identifier], dts: Seq[DataType]): Either[LatisException, Tuple] =
    dts.toList match {
      case e1 :: e2 :: es => fromElements(id, e1, e2, es *)
      case _ => LatisException("Tuple must have at least two elements").asLeft
    }

  def fromSeq(id: Identifier, dts: Seq[DataType]): Either[LatisException, Tuple] = fromSeq(Some(id), dts)

  def fromSeq(dts: Seq[DataType]): Either[LatisException, Tuple] = fromSeq(None, dts)

}

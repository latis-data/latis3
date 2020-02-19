package latis.util

import cats.implicits._
import latis.data.Datum
import latis.model.ValueType

object DataUtils {

  /**
   * Tries to convert a sequence of any values into a sequence of Datum.
   * The values' types must be a supported ValueType and each element
   * must be of the same type.
   */
  def anySeqToDatumSeq(vs: Seq[Any]): Either[LatisException, Seq[Datum]] = vs.length match {
    case 0 => Right(Seq.empty)
    case _ =>
      // Makes sure each element has the same type
      ValueType.fromValue(vs.head).flatMap { vtype =>
        vs.toVector.traverse(v => vtype.makeDatum(v))
      }
  }
}

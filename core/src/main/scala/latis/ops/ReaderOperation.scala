package latis.ops

import java.net.URI

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import latis.data.Text
import latis.data.TupleData
import latis.dataset.Dataset
import latis.input.DatasetReader
import latis.model.DataType
import latis.model.Function
import latis.util.LatisException

/**
 * Defines an operation that applies the given DatasetReader
 * to substitute the URI variable in the original dataset.
 * This assumes that the only variable in the range is a
 * text variable representing a dataset URI that the reader
 * can turn into a dataset.
 */
case class ReaderOperation(reader: DatasetReader, ops: Seq[UnaryOperation] = Seq.empty) extends MapRangeOperation {
  //TODO: update metadata

  // not enough to avoid reader serialization
  val f: URI => Dataset = reader.read

  def mapFunction(model:  DataType): TupleData => TupleData = {
    //TODO: avoid reader in the closure, needs to be serialized for spark
    (td: TupleData) => td.elements.head match {
      case Text(u) =>
        val uri = Try(new URI(u)) match {
          case Success(uri) => uri
          case Failure(e) =>
            val msg = s"Invalid URI: $u"
            throw LatisException(msg, e)
        }
        //TupleData(reader.read(uri).withOperations(ops).unsafeForce().data)
        TupleData(f(uri).withOperations(ops).unsafeForce().data)
        //TODO: deal will errors from unsafeForce
      case _ =>
        val msg = "URI variable must be of type text"
        throw LatisException(msg)
    }
  }

  def applyToModel(model: DataType): DataType = model match {
    case Function(d, _) => Function(d, reader.model)
  }
}

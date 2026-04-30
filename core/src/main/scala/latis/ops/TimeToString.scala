package latis.ops

import cats.syntax.all.* 

import latis.data.*
import latis.data.Data.*
import latis.model.*
import latis.util.Identifier
import latis.util.LatisException

case class TimeToString(id: Identifier) extends MapOperation {
  def mapFunction(model: DataType): Sample => Sample = {
    case Sample(domain, RangeData(time)) =>
      val newtime = time match {
        case Integer(t) =>
          val timestring: String = t.toString
          StringValue(timestring)
        case _ => 
          throw LatisException("Invalid range type for TimeToString")
      }
      Sample(domain, Seq(newtime))
  }

  override def applyToModel(model: DataType): Either[LatisException, DataType] = {
    Either.catchOnly[LatisException](model.map {
      case s: Scalar if (s.id == id) =>
        val md = s.metadata + ("type", StringValueType.toString) + ("units", "yyyyDDD")

        Scalar.fromMetadata(md) match {
          case Left(err) => 
            println(err)
            throw LatisException("Error updating scalar with new metadata")
          case Right(d) => d
        }
      case dt => dt
    })
  }
}

object TimeToString {
  def builder: OperationBuilder = (args: List[String]) => fromArgs(args)

  def fromArgs(args: List[String]): Either[LatisException, TimeToString] = args match {
    case arg :: Nil =>
      Either.fromOption(Identifier.fromString(arg), LatisException(s"'$arg' is not a valid identifier")).map(TimeToString(_))
    case _ => Left(LatisException("TimeToString requires one argument"))
  }
}
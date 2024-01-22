package latis.util

import latis.data.Text
import latis.model.*
import latis.ops.*
import latis.time.Time
import latis.util.dap2.parser.ast

case class SqlBuilder(
  table: String,
  columns: List[Column],
  selections: List[String],
  otherPredicate: Option[String],
  limit: Option[Int],
  model: DataType,
  order: String,
  vendor: Option[String]
) {

  def addOp(op: UnaryOperation): SqlBuilder = op match {
    case count: CountAggregation =>
      val newColumns = List(Column("count(*)"))
      val newModel = count.applyToModel(model).fold(throw _, identity)
      copy(columns = newColumns, model = newModel, order = "")

    case _: Head =>
      val newLimit = limit match {
        case Some(n) => Some(Math.min(n, 1))
        case None    => Some(1)
      }
      copy(limit = newLimit)

    case Take(cnt) =>
      val newLimit = limit match {
        case Some(n) => Some(Math.min(n, cnt))
        case None    => Some(cnt)
      }
      copy(limit = newLimit)

    case Rename(origId, newId) =>
      val newColumns = columns.map { col =>
        //TODO: disallow rename on old name? should latis prevent?
        if ((col.name +: col.rename.toList).contains(origId.asString))
          Column(col.name, Some(newId.asString))
        else col
      }
      val newModel = op.applyToModel(model).fold(throw _, identity)
      copy(columns = newColumns, model = newModel)

    case _: Projection =>
      val newModel = op.applyToModel(model).fold(throw _, identity)
      val newColumns = {
        //Need to preserve renames
        val projected = newModel.getScalars.filterNot(_.isInstanceOf[Index]).map(_.id.asString)
        columns.filter { col =>
          projected.contains(col.rename.getOrElse(col.name))
        }
      }
      copy(columns = newColumns, model = newModel)

    case Selection(id, sop, v) =>
      //TODO: always use first Time variable for "time" selection? or require rename
      //Always use original name for query
      val name = columns.find { col =>
        //TODO: disallow selecting on old name? latis shouldn't let us get that far?
        (col.name +: col.rename.toList).contains(id.asString)
      }.getOrElse {
        throw LatisException(s"Selection variable not found: ${id.asString}")
      }.name
      val value = model.findVariable(id) match {
        case Some(t: Time) =>
          // Interpret a time value as ISO or native numeric units.
          // If the database time column is non-numeric, this expects the time units
          // to reflect a format that works with the database.
          t.convertValue(v) match {
            case Right(Text(s)) => StringUtils.ensureSingleQuoted(s)
            case Right(n)       => n.asString
            case Left(le)       => throw LatisException(s"Invalid time value: $v", le)
          }
        case Some(s: Scalar) if (s.valueType == StringValueType) =>
          //Ensure that text variable value is quoted
          StringUtils.ensureSingleQuoted(v)
        case Some(_) => v
        case None =>
          //Should not get this far with an invalid id
          throw LatisException(s"Variable not found ${id.asString}")
      }
      val operator = sop match {
        //Note that unsupported operators are excluded in canHandleOperation
        //TODO: support EqTilde, NeEqTilde with "like"
        case ast.EqEq => "="
        case _        => ast.prettyOp(sop)
      }
      val newSelections = selections :+ s"$name $operator $value"
      copy(selections = newSelections)

    case _ => throw LatisException(s"Invalid operation $op")
  }

  def result: String = {
    val select = columns.map {
      case Column(name, Some(rename)) => s"$name AS $rename"
      case Column(name, None)         => name
    }.mkString("SELECT ", ", ", "")

    val from = s" FROM $table"

    val where = otherPredicate.toList ++ selections match {
      case Nil  => ""
      case list => list.mkString(" WHERE ", " AND ", "")
    }

    // TODO: More complete support for different databases.
    val lim = vendor match {
      case Some("SQLite") =>
        limit.map(n => s" LIMIT $n")
      case _ =>
        limit.map(n => s" FETCH FIRST $n ROWS ONLY")
    }

    select + from + where + order + lim.getOrElse("")
  }
}

object SqlBuilder {

  def buildQuery(
    table: String, //TODO: use model ID?
    model: DataType,
    ops: List[UnaryOperation] = List.empty,
    otherPredicate: Option[String] = None,
    vendor: Option[String] = None
  ): String = {
    // Order by domain variables even if not projected.
    // We must do it here before they are projected away.
    val order = model match {
      case Function(domain, _) =>
        domain.getScalars.filterNot(_.isInstanceOf[Index]).map(_.id.asString) match {
          case Nil  => ""
          case list => list.mkString(" ORDER BY ", ", ", " ASC")
        }
      case _ => ""  //not a Function, nothing to order by
    }

    // Define initial SqlBuilder with no operations applied.
    val init: SqlBuilder = SqlBuilder(
      table = table,
      columns = model.getScalars.filterNot(_.isInstanceOf[Index]).map(s => Column(s.id.asString)),
      selections = List.empty,
      otherPredicate = otherPredicate,
      limit = None,
      model = model,
      order = order,
      vendor = vendor
    )

    ops.foldLeft(init)((b, op) => b.addOp(op)).result
  }

}

case class Column(name: String, rename: Option[String] = None)

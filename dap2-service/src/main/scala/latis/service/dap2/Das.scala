package latis.service.dap2

import cats.data.NonEmptyList
import org.typelevel.paiges.Doc

import latis.dataset.Dataset
import latis.model._
import latis.util.Identifier
import latis.util.Identifier._

final case class Das(containers: List[Das.AttributeCont]) {
  val toDoc: Doc = {
    val prefix = Doc.text("Attributes {")
    val suffix = Doc.char('}')
    if (containers.nonEmpty)
      prefix / Doc.intercalate(Doc.line, containers.map(_.toDoc)).indent(2) / suffix
    else
      prefix / suffix
  }
  val asString: String = toDoc.render(1)
}

object Das {
  sealed trait AttributeCont {
    def toDoc: Doc
  }

  final case class NestedAttributeCont(id: Identifier, containers: List[AttributeCont]) extends AttributeCont {
    override val toDoc: Doc = {
      val prefix = Doc.text(id.asString) + Doc.space + Doc.char('{')
      val suffix = Doc.char('}')
      if (containers.nonEmpty)
        prefix / Doc.intercalate(Doc.line, containers.map(_.toDoc)).indent(2) / suffix
      else
        prefix / suffix
    }
  }

  final case class ItemAttributeCont(id: Identifier, attributes: List[Attribute[_,_]]) extends AttributeCont {
    override val toDoc: Doc = {
      val prefix = Doc.text(id.asString) + Doc.space + Doc.char('{')
      val suffix = Doc.char('}')
      if (attributes.nonEmpty)
        prefix / Doc.intercalate(Doc.line, attributes.map(_.toDoc)).indent(2) / suffix
      else
        prefix / suffix
    }
  }

  case class Attribute[A<:AtomicType[F],F](id: Identifier, ty: A, values: NonEmptyList[F]) {
    val toDoc: Doc = Doc.str(ty) + Doc.space + Doc.text(id.asString) + Doc.space +
      Doc.text(values.tail.foldLeft(ty.asDasString(values.head))((acc, v) => acc + ", " + ty.asDasString(v))) +
      Doc.char(';')
  }

  private[dap2] def fromDataType(dt: DataType, root: Boolean = false): AttributeCont = dt match {
    case s: Scalar => ItemAttributeCont(
      s.id,
      s.metadata.properties.toList.filterNot(prop => prop._1 == "id" || prop._1 == "class" || prop._1 == "type").map { prop =>
        Attribute(
          Identifier.fromString(prop._1).get, // Should be safe, since the Identifier is being constructed from a source with a valid Identifier
          AtomicType.String,
          NonEmptyList.one(prop._2)
        )
      }
    )
    case t: Tuple => NestedAttributeCont(
      t.id.getOrElse(id"unknown"),
      t.elements.map(fromDataType(_))
    )
    case f: Function => NestedAttributeCont(
      f.id.getOrElse(if (root) id"samples" else id"unknown"),
      List(fromDataType(f.domain), fromDataType(f.range))
    )
  }

  def fromDataset(dataset: Dataset): Das = Das(List(fromDataType(dataset.model, true)))
}

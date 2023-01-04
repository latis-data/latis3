package latis.service.dap2

import cats.syntax.all._
import org.typelevel.paiges.Doc

import latis.dataset.Dataset
import latis.model._
import latis.service.dap2.AtomicType._
import latis.util.Identifier
import latis.util.Identifier.IdentifierStringContext
import latis.util.LatisException

final case class Dds(id: Identifier, typeDecls: List[Dds.TypeDecl]) {
  val toDoc: Doc = {
    val prefix = Doc.text("Dataset {")
    val suffix = Doc.char('}') + Doc.space + Doc.text(id.asString) + Doc.char(';')
    val body = Doc.intercalate(Doc.line, typeDecls.map(_.toDoc)).indent(2)
    prefix / body / suffix
  }
  val asString: String = toDoc.render(1)
}

/* TODO: Support DDS's Array and Grid type declarations */
object Dds {
  sealed trait TypeDecl {
    def toDoc: Doc
  }
  final case class AtomicDecl(id: Identifier, ty: AtomicType[_]) extends TypeDecl {
    override val toDoc: Doc = Doc.str(ty) + Doc.space + Doc.text(id.asString) + Doc.char(';')
  }
  final case class StructureDecl(id: Identifier, typeDecls: List[TypeDecl]) extends TypeDecl {
    override val toDoc: Doc = {
      val prefix = Doc.text("Structure {")
      val suffix = Doc.char('}') + Doc.space + Doc.text(id.asString) + Doc.char(';')
      val body = Doc.intercalate(Doc.line, typeDecls.map(_.toDoc)).indent(2)
      prefix / body / suffix
    }
  }
  final case class SequenceDecl(id: Identifier, typeDecls: List[TypeDecl]) extends TypeDecl {
    override val toDoc: Doc = {
      val prefix = Doc.text("Sequence {")
      val suffix = Doc.char('}') + Doc.space + Doc.text(id.asString) + Doc.char(';')
      val body = Doc.intercalate(Doc.line, typeDecls.map(_.toDoc)).indent(2)
      prefix / body / suffix
    }
  }

  private def fromScalar(scalar: Scalar): Either[LatisException, AtomicDecl] = {
    val id = scalar.id
    AtomicType.fromScalar(scalar).map(AtomicDecl(id, _))
  }

  private def fromTuple(tuple: Tuple): Either[LatisException, StructureDecl] =
    tuple.elements.traverse(fromDataType(_))
      .map(StructureDecl(tuple.id.getOrElse(id"unknown"), _))

  private def fromFunction(func: Function, root: Boolean): Either[LatisException, SequenceDecl] =
    List(fromDataType(func.domain), fromDataType(func.range))
      .sequence
      .map(SequenceDecl(func.id.getOrElse(if (root) id"samples" else id"unknown"), _))

  private[dap2] def fromDataType(dt: DataType, root: Boolean = false): Either[LatisException, TypeDecl] = dt match {
    case s: Scalar => fromScalar(s)
    case t: Tuple => fromTuple(t)
    case f: Function => fromFunction(f, root)
  }

  def fromDataset(dataset: Dataset): Either[LatisException, Dds] =
    fromDataType(dataset.model, true)
      .map(typeDecl => Dds(dataset.id.getOrElse(id"unknown"), List(typeDecl)))
}

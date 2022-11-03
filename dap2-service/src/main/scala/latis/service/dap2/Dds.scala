package latis.service.dap2

import cats.syntax.all._
import org.typelevel.paiges.Doc

import latis.dataset.Dataset
import latis.model._
import latis.util.Identifier
import latis.util.Identifier.IdentifierStringContext
import latis.util.LatisException

final case class Dds(id: Identifier, typeDecls: List[Dds.TypeDecl]) {
  override val toString: String = {
    val prefix = Doc.text("Dataset {")
    val suffix = Doc.char('}') + Doc.space + Doc.text(id.asString) + Doc.char(';')
    val body = Doc.intercalate(Doc.line, typeDecls.map(_.toDoc)).indent(2)
    val doc = prefix / body / suffix
    doc.render(1)
  }
}

object Dds {
  sealed trait TypeDecl {
    def toDoc: Doc
  }
  final case class AtomicDecl(id: Identifier, ty: AtomicType) extends TypeDecl {
    override val toDoc: Doc = Doc.str(ty) + Doc.space + Doc.text(id.asString) + Doc.char(';')
  }
  final case class ArrayDecl(/* TODO: Not needed yet */)
  final case class StructureDecl(id: Identifier, typeDecls: List[TypeDecl]) extends TypeDecl {
    override val toDoc = {
      val prefix = Doc.text("Structure {")
      val suffix = Doc.char('}') + Doc.space + Doc.text(id.asString) + Doc.char(';')
      val body = Doc.intercalate(Doc.line, typeDecls.map(_.toDoc)).indent(2)
      prefix / body / suffix
    }
  }
  final case class SequenceDecl(id: Identifier, typeDecls: List[TypeDecl]) extends TypeDecl {
    override val toDoc = {
      val prefix = Doc.text("Sequence {")
      val suffix = Doc.char('}') + Doc.space + Doc.text(id.asString) + Doc.char(';')
      val body = Doc.intercalate(Doc.line, typeDecls.map(_.toDoc)).indent(2)
      prefix / body / suffix
    }
  }
  final case class GridDecl(/* TODO: Not needed yet */)

  sealed trait AtomicType {
    override def toString = this.getClass.getSimpleName
  }
  final case class Byte() extends AtomicType
  final case class Int16() extends AtomicType
  final case class UInt16() extends AtomicType
  final case class Int32() extends AtomicType
  final case class UInt32() extends AtomicType
  final case class Float32() extends AtomicType
  final case class Float64() extends AtomicType
  final case class String() extends AtomicType
  final case class Url() extends AtomicType

  def fromScalar(scalar: Scalar): Either[LatisException, AtomicDecl] = {
    val id = scalar.id
    scalar.valueType match {
      case _: DoubleValueType.type => Right(AtomicDecl(id, Float64()))
      case _: FloatValueType.type => Right(AtomicDecl(id, Float32()))
      case _: IntValueType.type => Right(AtomicDecl(id, Int32()))
      case _: ShortValueType.type => Right(AtomicDecl(id, Int16()))
      case _: ByteValueType.type => Right(AtomicDecl(id, Byte()))
      case _: StringValueType.type => Right(AtomicDecl(id, String()))
      case _ => Left(LatisException("Scalar could not be parsed to a DDS atomic type."))
    }
  }

  def fromTuple(tuple: Tuple): Either[LatisException, StructureDecl] = {
    tuple.elements.map { elem => fromDataType(elem) }
      .traverse(identity)
      .map { lst => StructureDecl(tuple.id.getOrElse(id"unknown"), lst) }
  }

  def fromFunction(func: Function): Either[LatisException, SequenceDecl] = {
    List(fromDataType(func.domain), fromDataType(func.range))
      .traverse(identity)
      .map { lst => SequenceDecl(func.id.getOrElse(id"unknown"), lst) }
  }

  def fromDataType(dt: DataType): Either[LatisException, TypeDecl] = dt match {
    case s: Scalar => fromScalar(s)
    case t: Tuple => fromTuple(t)
    case f: Function => fromFunction(f)
  }

  def fromDataset(dataset: Dataset): Either[LatisException, Dds] = {
    fromDataType(dataset.model)
      .map { typeDecl => Dds(dataset.id.getOrElse(id"unknown"), List(typeDecl))}
  }
}

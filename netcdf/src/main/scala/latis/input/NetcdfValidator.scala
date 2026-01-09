package latis.input

import cats.syntax.all.*
import ucar.ma2.{DataType as NcType}
import ucar.nc2.NetcdfFile

import latis.model.*

object NetcdfValidator {

  /**
   * Generates error messages for unsupported models.
   *
   * This expects that the model is a Function with no nested Functions.
   */
  def validateModel(model: DataType): Either[List[String], Unit] = {
    //TODO: consider returning ValidatedNec[String, DataType]
    val errs = scala.collection.mutable.ListBuffer[String]()

    model match {
      case f: Function =>
        if (f.range.exists(_.isInstanceOf[Function]))
          errs += "NetcdfAdapter does not support nested Functions"
        if (f.arity > 3)
          errs += "NetcdfAdapter does not support more than 3 dimensions"
      case _ =>
        errs += "NetcdfAdapter expects a Function"
    }

    if (errs.isEmpty) ().asRight
    else errs.toList.asLeft
  }

  /**
   * Generates error messages for inconsistencies between the model and netCDF file.
   *
   * This expects that the variables exist with the expected types and shapes.
   */
  def validateNetcdfFile(ncFile: NetcdfFile, model: DataType): Either[List[String], Unit] = {
    for {
      _ <- validateVariableNames(ncFile, model)
      _ <- validateTypes(ncFile, model)
      _ <- validateShapes(ncFile, model)
    } yield ()
  }

  /** Generates error messages for variables that are not found in the netCDF file */
  private def validateVariableNames(ncFile: NetcdfFile, model: DataType): Either[List[String], Unit] =
    model.getScalars.filter {
      case i: Index  => ncFile.findDimension(i.ncName) == null
      case s: Scalar => ncFile.findVariable(s.ncName) == null
    }.map(s => s"Variable not found: ${s.ncName}") match { //TODO: unescape?
      case Nil  => ().asRight
      case list => list.asLeft
    }

  /**
   * Generates error messages for variables with mismatched types.
   * Assumes that the variable names have already been validated.
   */
  private def validateTypes(ncFile: NetcdfFile, model: DataType): Either[List[String], Unit] =
    model.nonIndexScalars.map { s =>
      val name = s.ncName
      val v = ncFile.findVariable(name)
      if (matchTypes(s.valueType, v.getDataType)) None
      else s"Types don't match for variable $name: ${s.valueType}, ${v.getDataType}".some
    }.unite match {
      case Nil  => ().asRight
      case list => list.asLeft
    }

  /**
   * Generates error messages for variables with inconsistent shapes.
   * The shape of range variables should be the Cartesian product
   * of 1D domain variable sizes.
   *
   * Assumes that the variable names have already been validated.
   */
  private def validateShapes(ncFile: NetcdfFile, model: DataType): Either[List[String], Unit] = {
    val domainScalars = model match {
      case Function(d, _) => d.getScalars
      case _ => List.empty
    }
    val rangeScalars: List[Scalar] = model match {
      case Function(_, r) => r.getScalars
      case _ => List.empty
    }

    // Domain variable must be one-dimensional
    val invalidDomains: List[String] =
      domainScalars.filterNot(_.isInstanceOf[Index]).map(_.ncName).map { name =>
        if (ncFile.findVariable(name).getRank == 1) None
        else Some(s"Domain variable is not 1D: $name")
      }.unite

    if (invalidDomains.nonEmpty) invalidDomains
    else {
      val shape: List[Int] = domainScalars.map {
        case i: Index  => ncFile.findDimension(i.ncName).getLength
        case s: Scalar => ncFile.findVariable(s.ncName).getShape.head
      }
      rangeScalars.map(_.ncName).map { name =>
        val ncshape = ncFile.findVariable(name).getShape.toList
        if (ncshape == shape) None
        else Some(s"Variable shape does not match for $name: $ncshape, expected: $shape")
      }.unite
    }
  } match {
    case Nil  => ().asRight
    case list => list.asLeft
  }

  /** Determines if the model and file types are consistent. */
  private def matchTypes(latisType: ValueType, ncType: NcType): Boolean = {
    (latisType, ncType) match {
      case (BooleanValueType, NcType.BOOLEAN) => true
      case (ByteValueType,    NcType.BYTE)    => true
      case (ShortValueType,   NcType.UBYTE)   => true
      case (CharValueType,    NcType.CHAR)    => true
      case (ShortValueType,   NcType.SHORT)   => true
      case (IntValueType,     NcType.USHORT)  => true
      case (IntValueType,     NcType.INT)     => true
      case (LongValueType,    NcType.UINT)    => true
      case (LongValueType,    NcType.LONG)    => true
      case (DoubleValueType,  NcType.ULONG)   => true
      case (DoubleValueType,  NcType.DOUBLE)  => true
      case (FloatValueType,   NcType.FLOAT)   => true
      case (StringValueType,  NcType.STRING)  => true
      case _ => false
    }
  }
}

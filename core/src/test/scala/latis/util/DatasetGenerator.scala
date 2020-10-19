package latis.util

import java.util.UUID

import atto.Atto._
import atto.ParseResult
import atto.Parser

import latis.data.Data._
import latis.data._
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model._

/**
 * Provides convenient methods for constructing Datasets for testing.
 */
object DatasetGenerator {
  //TODO: add to DSL

  /**
   * Generates a dataset of a few samples with the model specified in the input
   * string. The values, starting with the first sample, increment depending on
   * their type. Domain variables increment independently.
   * {{{
   *   > DatasetGenerator("double -> (double, boolean)")
   * }}}
   * returns a dataset where the first two samples are
   * 0.0 -> (0.0, true)
   * 1.0 -> (1.0, false)
   * If a multi-dimensional dataset is specified, then a cartesian dataset is
   * returned where the first variable in the domain is of size 2, the second
   * size 3, etc.
   * {{{
   *   > DatasetGenerator("(string, int) -> (double, double)")
   * }}}
   * returns a dataset where the samples are:
   * ("a", 0) -> (0.0, 1.0)
   * ("a", 1) -> (2.0, 3.0)
   * ("a", 2) -> (4.0, 5.0)
   * ("b", 0) -> (6.0, 7.0)
   * ("b", 1) -> (8.0, 9.0)
   * ("b", 2) -> (10.0, 11.0)
   *
   */
  def apply(typeSig: String): MemoizedDataset = {
    val model = modelFromString(typeSig)
    val ds = new MemoizedDataset(
      Metadata(makeDatasetID()),
      model,
      generateData(model)
    )
    ds
  }

  private def modelFromString(s: String): DataType = {
    val absModel = modelParser.model.parseOnly(s) match {
      case ParseResult.Done(_, m) => m
      case _ => throw LatisException(s"failed to parse $s")
    }
    modelFromAst(absModel)
  }

  private def modelFromAst(absModel: modelAst.Model): DataType = absModel match {
    case modelAst.Function(d, r) => Function(modelFromAst(d), modelFromAst(r))
    case modelAst.Tuple(l) => Tuple(l.map(modelFromAst))
    case modelAst.Scalar(t) => Scalar(Metadata(t.toString) + ("type" -> t.toString))
  }

  private def generateData(m: DataType): MemoizedFunction = m.arity match {
    case 1 => generateData1D(m)
    case 2 => generateData2D(m)
    case 3 => generateData3D(m)
    case a => throw LatisException(s"DatasetGenerator doesn't support datasets of arity $a")
  }

  private def generateData1D(m: DataType): MemoizedFunction = {
    val scalarTypes = m.getScalars.map(_.valueType)
    val dGen = DataGenerator(scalarTypes.head)
    val rGen = new RangeGenerator(m)
    val numSamples = 3
    val samples = for {
      d <- dGen.take(numSamples).toList
    } yield {
      Sample(DomainData(d), rGen.next())
    }
    SampledFunction(samples)
  }

  private def generateData2D(m: DataType): MemoizedFunction = {
    val scalarTypes = m.getScalars.map(_.valueType)
    val d1s = DataGenerator(scalarTypes.head).take(2).toList
    val d2s = DataGenerator(scalarTypes(1)).take(3).toList
    val rGen = new RangeGenerator(m)
    val samples = for {
      d1 <- d1s
      d2 <- d2s
    } yield {
      Sample(DomainData(d1, d2), rGen.next())
    }
    SampledFunction(samples)
  }

  private def generateData3D(m: DataType): MemoizedFunction = {
    val scalarTypes = m.getScalars.map(_.valueType)
    val d1s = DataGenerator(scalarTypes.head).take(2).toList
    val d2s = DataGenerator(scalarTypes(1)).take(3).toList
    val d3s = DataGenerator(scalarTypes(2)).take(4).toList
    val rGen = new RangeGenerator(m)
    val samples = for {
      d1 <- d1s
      d2 <- d2s
      d3 <- d3s
    } yield {
      Sample(DomainData(d1, d2, d3), rGen.next())
    }
    SampledFunction(samples)
  }

  def generate1DDataset(
    xs: Seq[Any],
    rs: Seq[Any]*
  ): MemoizedDataset = {
    val md    = Metadata(makeDatasetID())
    val model = makeModel(Seq(xs.head), rs.map(_.head))
    val data  = CartesianFunction1D.fromValues(xs, rs: _*).toTry.get
    new MemoizedDataset(md, model, data)
  }

  def generate2DDataset(
    xs: Seq[Any],
    ys: Seq[Any],
    rs: Seq[Seq[Any]]*
  ): MemoizedDataset = {
    val md    = Metadata(makeDatasetID())
    val model = makeModel(Seq(xs.head, ys.head), rs.map(_.head.head))
    val data  = CartesianFunction2D.fromValues(xs, ys, rs: _*).toTry.get
    new MemoizedDataset(md, model, data)
  }

  def generate3DDataset(
    xs: Seq[Any],
    ys: Seq[Any],
    zs: Seq[Any],
    rs: Seq[Seq[Seq[Any]]]*
  ): MemoizedDataset = {
    val md    = Metadata(makeDatasetID())
    val model = makeModel(Seq(xs.head, ys.head, zs.head), rs.map(_.head.head.head))
    val data  = CartesianFunction3D.fromValues(xs, ys, zs, rs: _*).toTry.get
    new MemoizedDataset(md, model, data)
  }

  def makeDatasetID(): String =
    "dataset_" + UUID.randomUUID().toString.take(8)

  //use 1st value of each variable
  def makeModel(ds: Seq[Any], rs: Seq[Any]): DataType =
    Function(makeDomainType(ds), makeRangeType(rs))

  def makeDomainType(data: Seq[Any]): DataType = {
    val scalars = data.zipWithIndex.map {
      case (v, i) =>
        val vname = s"_${i + 1}"
        val vtype = ValueType.fromValue(v).toTry.get.toString
        Scalar(
          Metadata(
            "id"   -> vname,
            "type" -> vtype
          )
        )
    }
    scalars.length match {
      //TODO: DataType.fromSeq, like Data.fromSeq but keep nested Tuples
      case 0 => ??? //error
      case 1 => scalars.head
      case _ => Tuple(scalars)
    }
  }

  def makeRangeType(data: Seq[Any]): DataType = {
    val scalars = data.zipWithIndex.map {
      case (v, i) =>
        val vname = (i + 97).toChar.toString //letters a, b, ...
        val vtype = ValueType.fromValue(v).toTry.get.toString
        Scalar(
          Metadata(
            "id"   -> vname,
            "type" -> vtype
          )
        )
    }
    scalars.length match {
      case 0 => ??? //error
      case 1 => scalars.head
      case _ => Tuple(scalars)
    }
  }
}

class BooleanGenerator extends Iterator[Datum] {
  def hasNext: Boolean = true
  private var lastValue = false
  def next(): BooleanValue = {
    lastValue = !lastValue
    BooleanValue(lastValue)
  }
}

class IntGenerator extends Iterator[Datum] {
  def hasNext: Boolean = true
  private var lastValue = -1
  def next(): IntValue = {
    lastValue += 1
    IntValue(lastValue)
  }
}

class DoubleGenerator extends Iterator[Datum] {
  def hasNext: Boolean = true
  private var lastValue = -1.0
  def next(): DoubleValue = {
    lastValue += 1.0
    DoubleValue(lastValue)
  }
}

class StringGenerator extends Iterator[Datum] {
  def hasNext: Boolean = true
  private var lastValue = "`"
  def next(): StringValue = {
    val nextAscii = if (lastValue == 'z') 97 else lastValue.head.toInt + 1
    lastValue = nextAscii.toChar.toString
    StringValue(lastValue)
  }
}

object DataGenerator {
  def apply(vType: ValueType): Iterator[Datum] = vType match {
    case BooleanValueType => new BooleanGenerator
    case IntValueType => new IntGenerator
    case DoubleValueType => new DoubleGenerator
    case StringValueType => new StringGenerator
    case t => throw LatisException(s"No generator for $t")
  }
}

class RangeGenerator(model: DataType) extends Iterator[RangeData] {
  def hasNext: Boolean = true
  private val boolGen = DataGenerator(BooleanValueType)
  private val intGen = DataGenerator(IntValueType)
  private val doubleGen = DataGenerator(DoubleValueType)
  private val stringGen = DataGenerator(StringValueType)

  private def nextOfType(t: ValueType): Datum = t match {
    case BooleanValueType => boolGen.next()
    case IntValueType => intGen.next()
    case DoubleValueType => doubleGen.next()
    case StringValueType => stringGen.next()
    case t => throw LatisException(s"No generator for $t")
  }
  private val rangeTypes = model.getScalars.drop(model.arity).map(_.valueType)
  def next(): RangeData = RangeData(rangeTypes.map(nextOfType))
}

object modelParser {
  def model: Parser[modelAst.Model] =
    parens(function) | function | tuple | scalar

  /** Doesn't support nested functions in the domain. */
  def function: Parser[modelAst.Model] = for {
    d <- (tuple | scalar).token
    _ <- string("->").token
    r <- model.token
  } yield modelAst.Function(d, r)

  /** Only parses tuples of scalars */
  def tuple: Parser[modelAst.Model] = for {
    l <- parens(sepBy(scalar.token, char(',').token))
  } yield modelAst.Tuple(l)

  def scalar: Parser[modelAst.Model] = for {
    t <- valueType
  } yield modelAst.Scalar(t)

  def valueType: Parser[ValueType] =
    booleanType | intType | doubleType | stringType

  def booleanType: Parser[ValueType] = for {
    _ <- string("boolean") | string("Boolean")
  } yield BooleanValueType

  def intType: Parser[ValueType] = for {
    _ <- string("int") | string("Int")
  } yield IntValueType

  def doubleType: Parser[ValueType] = for {
    _ <- string("double") | string("Double")
  } yield DoubleValueType

  def stringType: Parser[ValueType] = for {
    _ <- string("string") | string("String")
  } yield StringValueType
}

object modelAst {
  sealed trait Model

  final case class Function(
    domain: Model, range: Model
  ) extends Model

  final case class Tuple(
    elements: List[Model]
  ) extends Model

  final case class Scalar(
    valueType: ValueType
  ) extends Model
}

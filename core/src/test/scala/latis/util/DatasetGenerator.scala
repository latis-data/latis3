package latis.util

import java.util.UUID

import latis.data.CartesianFunction1D
import latis.data.CartesianFunction2D
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.model.Tuple
import latis.model.ValueType

/**
 * Provides convenient methods for constructing Datasets for testing.
 */
object DatasetGenerator {
  //TODO: add to DSL
  //TODO: make scalacheck generators

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

package latis.ops

import cats.effect.IO
import fs2.Stream

import latis.data.*
import latis.dataset.Dataset
import latis.dataset.TappedDataset
import latis.metadata.Metadata
import latis.model.DataType
import latis.util.LatisException

/**
 * Defines an Operation that combines two Datasets into one.
 */
trait BinaryOperation extends Operation {

  /** Combines the models of two Datasets. */
  def applyToModel(
    model1: DataType, 
    model2: DataType
  ): Either[LatisException, DataType]

  /** Combines the Data of two Datasets. */
  def applyToData(
    model1: DataType,
    stream1: Stream[IO, Sample],
    model2: DataType,
    stream2: Stream[IO, Sample]
  ): Either[LatisException, Stream[IO, Sample]]


  /** Combines two datasets. */
  final def combine(ds1: Dataset, ds2: Dataset): Either[LatisException, Dataset] = {
    val md = {
      //TODO: combine other dataset metadata properties?
      val name1 = ds1.id.map(_.asString).getOrElse("dataset1")
      val name2 = ds2.id.map(_.asString).getOrElse("dataset2")
      val props = List(
        "id" -> s"${name1}_${name2}",
        "history" -> List (
          s"""$name1: ${ds1.metadata.getProperty("history")}""",
          s"""$name2: ${ds2.metadata.getProperty("history")}""",
          s"Join(name1, name2)"
        ).mkString(System.lineSeparator)
      )
      Metadata(props *)
    }
    for {
      model <- applyToModel(ds1.model, ds2.model)
      samples <- applyToData(ds1.model, ds1.samples, ds2.model, ds2.samples)
    } yield {
      TappedDataset(md, model, StreamFunction(samples))
    }
  }
  
  /** Partially applies the first dataset to get a unary operation. */
  final def partialApply(dataset: Dataset): UnaryOperation = {
    //TODO: need to update metadata like "combine" does, 
    //      but can't modify Dataset here
    
    new UnaryOperation {
      override def applyToModel(model: DataType): Either[LatisException, DataType] =
        BinaryOperation.this.applyToModel(dataset.model, model)

      override def applyToData(data: Data, model: DataType): Either[LatisException, Data] =
        BinaryOperation.this.applyToData(
          dataset.model,
          dataset.samples, 
          model, 
          data.samples
        ).map(StreamFunction.apply)
    }
  }
    
}

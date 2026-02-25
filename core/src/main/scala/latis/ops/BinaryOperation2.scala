package latis.ops

import cats.effect.IO
import fs2.Stream

import latis.data.Sample
import latis.model.DataType
import latis.util.LatisException


//TODO: replace BinaryOperation
//  this adds the models to applyToData
//  also experimenting with using a Stream instead of Data

/**
 * Defines an Operation that combines two Datasets into one.
 */
trait BinaryOperation2 extends Operation {
  /*
  TODO: consider dropping BinaryOperations for UnaryOperations
   that take the initial dataset as an argument.
   Then we could drop "Unary"
   Would this give up opportunities to fold a collection of datasets?
   */
  
  //TODO: combine(ds1, ds2)?

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

}

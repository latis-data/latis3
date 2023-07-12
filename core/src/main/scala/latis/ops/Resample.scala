package latis.ops

import cats.syntax.all.*

import latis.data.*
import latis.model.*
import latis.util.LatisException

/**
 * Defines an operation that resamples a Dataset on a given DomainSet.
 * The model of Functions will be unchanged. Constant variables will
 * be repeated for each domain value.
 */
case class Resample(dset: DomainSet) extends UnaryOperation {
  //TODO: allow interpolations here or rely on SF?

  /**
   * Resampling does not affect the model.
   */
  def applyToModel(model: DataType): Either[LatisException, DataType] = model match {
    case f: Function =>
      //TODO: assert that set types matches dataset domain types
      if (f.arity != dset.rank)
        Left(LatisException("Function domain and domain set must have same dimensions"))
      else Right(f)
    //Data that is not a Function needs domain added to it
    case d => Right(Function.from(dset.model, d).fold(throw _, identity))
  }

  /**
   * Apply the DomainSet to the given Data.
   */
  def applyToData(sf: Data, model: DataType): Either[LatisException, Data] =
    sf match {
      case sf: SampledFunction => sf.resample(dset)
      case data =>
        // Duplicate const for every domain value
        val range: IndexedSeq[RangeData] = Vector.fill(dset.length)(RangeData(data))
        SetFunction(dset, range).asRight
    }

}

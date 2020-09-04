package latis.ops

import latis.data._
import latis.model._
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
    case d => Right(Function(dset.model, d))
  }

  /**
   * Apply the DomainSet to the given SampledFunction.
   */
  def applyToData(sf: SampledFunction, model: DataType): Either[LatisException, SampledFunction] = sf match {
    case ConstantFunction(data) =>
      // Duplicate const for every domain value
      val range: IndexedSeq[RangeData] = Vector.fill(dset.length)(RangeData(data))
      Right(SetFunction(dset, range))
    case sf: SampledFunction =>
      sf(dset)
  }

}

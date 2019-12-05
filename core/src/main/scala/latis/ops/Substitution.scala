package latis.ops

import latis.data._
import latis.model._

/**
 * Replaces a variable in a Dataset by using it to evaluate another Dataset.
 */
case class Substitution() extends BinaryOperation {
  //TODO: explore consequences of complex types
  //  this generally assumes ds2 is scalars a -> b but could be any type
  //TODO: require that ds2 range is ordered (monotonic) if replacing a domain value
  //TODO: if ds1 is memoized, evaluate with the entire domain set?
  //TODO: support aliases? or construct with id to replace?

  /*
   * TODO: name the target variable instead of matching names
   * or do in terms of sample path?
   * require rename
   * provide in smart constructor
   */

  /*
   * eval could be expensive
   * we know that both share the same ordering and have the same domain set, thus substitution and not resampling
   * can Substitution simply replace without eval?
   *
   * eval can be cheap if we use an Array SF
   *   should this automatically do the restructuring or leave it to the user?
   * handy if we do stride on data, can still eval full geoloc dataset
   *
   * 2D: sample does not preserve tuples so we can't get a SamplePosition for the Tuple
   * the 2 vars must be together
   *   e.g. sub (ix, iy) -> (lon, lat)
   *   (w, ix, iy, z) -> f   is ok
   * can the order be reversed? probably
   * note that substitution in the domain might not have expected ordering
   * would we be better off requiring currying such that we have a dedicated 2D domain
   *   for a Function with a special topology?
   */

//  /**
//   * Apply second Dataset to the first replacing the variable matching the domain
//   * variable of the second with its range variable.
//   */
//  def apply(ds1: Dataset, ds2: Dataset): Dataset = {
//    val model = applyToModel(ds1.model, ds2.model)
//
//    val data = applyToData(ds1, ds2)
//
//    //TODO: update Metadata
//    Dataset(ds1.metadata, model, data)
//  }

//  /*
//   * Splice a Seq[A]:
//   * slice: Seq[A]
//   * f: Seq[A] => Seq[A]
//   * splice(slice: Seq[A])(f: Seq[A] => Seq[A])
//   */
//  def splice(seq:Seq[_], slice: Seq[_], sub: Seq[_]): Seq[_] = {
//    seq.indexOfSlice(slice) match {
//      case -1 => ??? //slice not found, error or return orig?
//      case index => seq.splitAt(index) match {
//        case (p1, p2) => p1 ++ sub ++ p2.drop(slice.length)
//      }
//    }
//  }

  def applyToData(
    model1: DataType,
    data1: SampledFunction,
    model2: DataType,
    data2: SampledFunction
  ): SampledFunction = {
    // Previous processing should have ensured that the model is as expected
    // e.g. the expected values exist sequentially so we can use the index of the first.

    val vids = getDomainVariableIDs(model2)

    // Get the sample position of the first ds2 domain variable in ds1.
    //val pos: SamplePosition = ds1.model.getPath(vids.head) match {
    val path: SamplePath = model1.getPath(vids.head) match {
      //TODO: findPath? Option vs empty path
      case Some(p) => p
      case None    => ??? //error, variable not found in ds1
    }

    // Get the SampledFunction of ds2 to be used for evaluation
    val sf = data2

    // Make a function to modify a ds1 Sample by replacing the value
    // from evaluating ds2 with the value of the matching variable in ds1
    //val f: Sample => Sample = (sample: Sample) => path match {
    def f(sample: Sample, path: SamplePath): Sample = path match {
      /*
       * TODO: support nested Functions.
       * use full path, length indicates how deep
       * recurse down path
       * rebuild on the way out
       */
      case (head :: tail) if (tail.isEmpty) =>
        head match {
          case DomainPosition(i) =>
            val vals  = sample.domain
            val slice = vals.slice(i, i + vids.length) //get values to be replaced
            val sub   = sf(DomainData(slice)).get //TODO: handle bad eval
            /*
             * TODO: the range data will be sub'd into the domain so it must be Ordered Data
             */
            val vals2 = vals.splitAt(i) match {
              case (p1, p2) => p1 ++ sub ++ p2.drop(slice.length) //splice in new data
            }
            val domain = DomainData(vals2)
            Sample(domain, sample.range)
          case RangePosition(i) =>
            val vals = sample.range
            //get values to be replaced
            val slice = vals.slice(i, i + vids.length)
            /*
             * TODO: to use the range values to eval the sub ds, they must be Ordered Data
             */
            val sub = sf(DomainData(slice)).get //TODO: handle bad eval
            val vals2 = vals.splitAt(i) match {
              case (p1, p2) => p1 ++ sub ++ p2.drop(slice.length) //splice in new data
            }
            val range = RangeData(vals2)
            Sample(sample.domain, range)
        }
      // Tail not empty, recurse
      case (head :: tail) =>
        /*
         * head tells us whether to keep the domain or range and rebuild the other
         * Note, domain can't have nested functions so head must be a RangePosition.
         * we don't yet have nested function in a tuple, but we might?
         */
        sample.getValue(head) match {
          case Some(innerSF: SampledFunction) =>
            //assume no nested function in tuple
            val sf2 = innerSF.map(f(_, tail)) //recurse
            Sample(sample.domain, RangeData(sf2)) //assuming the nested function is not part of a tuple
          case _ => ??? //shouldn't get here unless path is invalid
        }
    }

    // Apply the substitution function to original data
    data1.map(f(_, path))
  }

  def applyToModel(dt1: DataType, dt2: DataType): DataType = {
    //TODO: prevent substituting a Function into the domain
    //  i.e. ds2 is not nested; OK in range

    // Get the substitution Dataset's scalar domain variable ids
    // and range variable.
    val vids  = getDomainVariableIDs(dt2)
    val range = dt2 match { case Function(_, r) => r }

    // Confirm that the target Dataset has the domain variables in sequence.
    // Note that the toSeq preserves all the nested data structures
    // and not just the scalars. Finding the vids consecutively
    // ensures that we are not crossing Tuple boundaries.
    //TODO: does protect against domain/range crossing if domain is scalar
    if (dt1.toSeq.map(_.id).indexOfSlice(vids) == -1) ??? //TODO: not found

    // Traverse the original model and replace the types matching the
    // substitution Dataset's domain with the types from its range.
    // Recursive helper function
    def go(dt: DataType): DataType = dt match {
      case s: Scalar =>
        if ((vids.length == 1) && (s.id == vids.head)) range
        else s
      case Tuple(es @ _*) =>
        es.map(_.id).indexOfSlice(vids) match {
          case -1 => Tuple(es.map(go): _*) //no match, keep recursing
          case index =>
            es.splitAt(index) match {
              // Splice in the new variable types
              case (p1, p3) =>
                val dts = (p1 :+ range ++ p3.drop(vids.length))
                if (dts.length == 1)
                  dts.head //Reduce 1-Tuple //TODO: Tuple constructor option? Tuple.reduced()? vs flattened
                else Tuple(dts: _*)
            }
        }
      case Function(d, r) => Function(go(d), go(r))
    }
    go(dt1)
  }

  /**
   * Convenience method to extract the variable IDs from the domain
   * of the Substitution Dataset.
   */
  private def getDomainVariableIDs(model: DataType): Seq[String] = model match {
    case Function(d, _) =>
      d match {
        case s: Scalar => Seq(s.id)
        case t: Tuple  => t.getScalars.map(_.id) //flattens nested Tuples
        case _ => ??? //TODO: can't have Function in domain
      }
    case _ => ??? //TODO invalid dataset type for ds2, must be Function
  }
}

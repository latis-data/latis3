package latis.ops

import cats.syntax.all._

import latis.data._
import latis.dataset.ComputationalDataset
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.model._
import latis.util.LatisException

/**
 * Replaces a variable in a Dataset by using it to evaluate another Dataset.
 */
case class Substitution(dataset: Dataset) extends MapOperation {
  //TODO: preserve nested tuples?

  def mapFunction(model: DataType): Sample => Sample = {
    // Get the paths to the substitution variables in the target Dataset
    //TODO: error if not consecutive
    val paths = modelScalars._1.toList.traverse { s =>
      val sId = s.id match {
        case Some(id) => id.asString
        case None => ""
      }
      model.getPath(sId)
    }.getOrElse {
      val msg = s"Failed to find substitution domain in target Dataset"
      throw LatisException(msg)
    }

    // Defines a function to modify a Sample by replacing the values
    // starting at the given path with the results of using them to
    // evaluate the substitution Dataset.
    def substitute(sample: Sample, path: SamplePath): Sample = path match {
      case Nil => ??? //empty path is not valid
      case pos :: Nil =>
        val f: Data => Either[LatisException, Data] = (data: Data) =>
          dataset match {
            case ComputationalDataset(_, _, f) => f(data)
            case mds: MemoizedDataset => DomainData.fromData(data).flatMap { dd =>
              mds.data.eval(dd).map(Data.fromSeq(_))
            }
            case _ =>
              val msg = "Substitution not enabled for non-memoized Datasets."
              throw LatisException(msg)
          }
        pos match {
          case DomainPosition(i) =>
            // Get the domain values
            val vals: List[Datum] = sample.domain
            // Extract the values to be replaced
            val slice: List[Datum] = vals.slice(i, i + domainVariableIDs.length)
            // Evaluate the substitution Dataset with the values to be replaced.
            val sub: List[Datum] = f(Data.fromSeq(slice)) match {
              case Right(rd) =>
                // Make sure these range data can be used for a domain, i.e. all Datum, no SF
                rd match {
                  case d: Datum => List(d)
                  case TupleData(ds @ _*) => ds.toList.map {
                    case d: Datum => d
                    case _ =>
                      throw LatisException("Domain substitution includes Function")
                  }
                  case _: SampledFunction =>
                    throw LatisException("Domain substitution includes Function")
                }
              case Left(le) => throw le
            }
            // Substitute the new values into the domain
            val domain: DomainData = vals.splitAt(i) match {
              case (p1, p2) => p1 ++ sub ++ p2.drop(slice.length)
            }
            Sample(domain, sample.range)
          case RangePosition(i) =>
            // Get the range values
            val vals: List[Data] = sample.range
            // Extract the values to be replaced; can't include Function
            val slice: List[Datum] = vals.slice(i, i + domainVariableIDs.length).map {
              case d: Datum => d
              // Note, there should be no TupleData in a Sample
              case _: TupleData =>
                throw LatisException("Substitution includes TupleData")
              case _: SampledFunction =>
                throw LatisException("Substitution includes Function")
            }
            // Evaluate the substitution Dataset with the values to be replaced
            val sub: List[Data] = f(Data.fromSeq(slice)) match {
              case Right(d: Datum) => List(d)
              case Right(TupleData(ds @ _*)) => ds.toList
              case Left(le) => throw le
            }
            // Substitute the new values into the range
            val range: RangeData = vals.splitAt(i) match {
              case (p1, p2) => p1 ++ sub ++ p2.drop(slice.length)
            }
            Sample(sample.domain, range)
        }
      // Tail not empty, recurse
      case pos :: tail =>
        // Get the nested function from the first part of the path and recurse
        sample.getValue(pos) match {
          case Some(innerSF: MemoizedFunction) =>
            val sf = SampledFunction(innerSF.sampleSeq.map(substitute(_, tail))) //recurse
            Sample(sample.domain, RangeData(sf))
          //TODO: support nested function in tuple
          case _ => ???
        }
    }

    (sample: Sample) => substitute(sample, paths.head)
  }

  def applyToModel(model: DataType): Either[LatisException, DataType] = {
    // Get the subDataset range Scalars.
    // Note, avoids nested tuples
    val subScalars = modelScalars._2

    // Traverse the original model and replace the types matching the
    // substitution Dataset's domain with the types from its range.
    // Recursive helper function
    def go(dt: DataType): DataType = dt match {
      case s: Scalar =>
        if ((domainVariableIDs.length == 1) && (s.id == domainVariableIDs.head)) subScalars.head
        else s
      case Tuple(es @ _*) =>
        //TODO: support aliases
        es.map(_.id).indexOfSlice(domainVariableIDs) match {
          case -1 => Tuple(es.map(go)) //no match, keep recursing
          case index =>
            es.splitAt(index) match {
              // Splice in the new variable types
              case (p1, p3) =>
                val dts = p1 ++ subScalars ++ p3.drop(domainVariableIDs.length)
                if (dts.length == 1) dts.head //Reduce 1-Tuple
                else Tuple(dts)
            }
        }
      case Function(d, r) => Function(go(d), go(r))
    }

    Right(go(model))
  }

  /**
   * Extracts the variable IDs from the domain of the Substitution Dataset.
   */
  private val modelScalars: (Seq[Scalar], Seq[Scalar]) = dataset.model match {
    case Function(d, r) => (d.getScalars, r.getScalars)
    case _ => throw LatisException("A substitution Dataset must be a Function.")
  }
  //TODO: factor out this exception into a smart constructor

  /**
   * Extracts the variable IDs from the domain of the Substitution Dataset.
   */
  private val domainVariableIDs: Seq[String] = modelScalars._1.map {
    _.id match {
      case Some(id) => id.asString
      case None => ""
    }
  }

}

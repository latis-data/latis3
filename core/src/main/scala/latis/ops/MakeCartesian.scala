package latis.ops

import scala.annotation.tailrec
import scala.collection.mutable

import cats.effect.IO
import cats.syntax.all.*
import cats.Eq
import cats.kernel.PartialOrder
import fs2.*

import latis.data.*
import latis.model.*
import latis.util.*

/**
 * Modify a Dataset such that the domain set is Cartesian.
 *
 * This gets all the distinct values for each domain variable in the
 * outer domain, makes a Cartesian product set, then fills each domain
 * point with an existing Sample or fill data. The result is a Cartesian
 * dataset (that could be rather sparse).
 *
 * The Dataset must have a multidimensional domain.
 * This reads all Samples into a List so there is risk of large
 * memory usage. This could be used to ensure that we have a Cartesian
 * dataset as required by some operation, but at a cost.
 */
case class MakeCartesian() extends StreamOperation {

  override def pipe(model: DataType): Pipe[IO, Sample, Sample] = {

    // Get the domain scalars and range type
    val (scalars, range) = model match {
      case Function(domain: Tuple, range) => (domain.getScalars, range)
      case _ =>
        val msg = "MakeCartesian requires a n > 1 dimensional Function"
        throw LatisException(msg)
    }

    // Define Eq for the outer domain, for finding matching samples
    given Eq[DomainData] = PartialOrder.fromPartialOrdering {
      CartesianDomainOrdering(scalars.map(_.ordering))
    }

    // Initial empty set of data for each domain scalar, for fold
    val zero = scalars.map { scalar =>
      val ord = LatisOrdering.partialToTotal(scalar.ordering)
      mutable.SortedSet.empty[Datum](ord)
    }

    // Process samples
    samples => {
      // Use List to process samples since we need to traverse it twice
      val io = samples.compile.toList.map { samples =>

        // Build a sorted set of data values for each domain variable
        val sets = samples.foldLeft(zero) {
          case (acc, Sample(ds, _)) =>
            //TODO: invalid sample with ds.size != ids.size
            ds.zipWithIndex.foldLeft(acc) {
              case (ss, (d, i)) => ss.updated(i, ss(i).addOne(d))
            }
        }

        // Construct a list of DomainData as a product set of each
        // set of domain scalar data values with the proper ordering.
        // i.e. all possible coordinates in a Cartesian grid.
        val dds = sets.foldLeft(List(List.empty[Datum])) { (acc, set) =>
          acc.flatMap(ds => set.toList.map(ds.appended))
        }

        // For each Cartesian point, find a matching Sample or fill.
        // Take advantage of ordering by pulling samples as needed.
        // Accumulate Samples for each domain point until we find an
        // existing Sample with that domain.
        @tailrec
        def go(pts: List[DomainData], orig: List[Sample], acc: List[Sample]): List[Sample] = {
          orig match {
            case (sample @ Sample(ds, _)) :: remainingSamples =>
              val (tofill, remainingPts) = pts.span { pt =>
                ! Eq[DomainData].eqv(ds, pt)
              }
              val fill = tofill.map { dd =>
                Sample(dd, RangeData(range.fillData))
              }

              // Recurse with the remaining Cartesian set of domain points
              // (minus the one we had a matching Sample for) pulling from
              // the remaining samples and adding fill data as needed.
              go(remainingPts.tail, remainingSamples, acc ++ fill :+ sample)

            case Nil => //no more original samples but keep filling
              acc ++ pts.map { dd =>
                Sample(dd, RangeData(range.fillData))
              }
          }
        }

        go(dds, samples, List.empty)
      }

      // Put the complete set of samples back into a Stream
      Stream.evals(io)
    }
  }

  // Model is unchanged
  //TODO: set Function topology to cartesian
  override def applyToModel(model: DataType): Either[LatisException, DataType] =
    model.asRight

}

package latis.ops

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import fs2.Pull
import fs2.Stream

import latis.data.*
import latis.dataset.*
import latis.metadata.Metadata
import latis.model.*
import latis.util.Identifier
import latis.util.LatisException


/**
 * A Join is a BinaryOperation that combines two or more Datasets.
 *
 * Unlike arbitrary binary operations, Joins provide lawful behavior
 * for distributing operations to the operands before applying the join. //TODO: true? or in Composite
 * This is often important for performance reasons.
 *
 * Properties of Joins:
 *  - Each dataset must have the same domain type.
 *  - Joins must not generate new variables.
 *  - Joins may add fill data (e.g. outer joins).
 *  - Joins may compute new values only to resolve duplication (e.g. average).
 *
 * The following classes of UnaryOperations can be distributed over the Join
 * operation and applied to the operands:
 *  - Filter (e.g. Selection)
 *  - MapOperation (e.g. Projection)
 *
 * Some classes of UnaryOperations can be applied to the operands
 * but also need to be reapplied after the join:
 *  - Taking (Head, Take, TakeRight, Last)
 *
 * Joins can be used by a CompositeDataset while enabling operation
 * push-down to member Datasets.
 */
trait Join2 extends Operation {
  //TODO: replace Join, this includes models in appyToData
  //TODO: use for CompositeDataset, needs to use join for model

  // Returns the new chunk and the remainder of the other two
  def joinChunks(
    model1: DataType,
    c1: Chunk[Sample],
    model2: DataType,
    c2: Chunk[Sample]
  ): (Chunk[Sample], Chunk[Sample], Chunk[Sample])

  def applyToModel(model1: DataType, model2: DataType): Either[LatisException, DataType]

  //TODO: do in terms of Stream?
  def applyToData(
    model1: DataType,
    data1: Data,
    model2: DataType,
    data2: Data
  ): Either[LatisException, Data] = {
    def go(
      leg1: Stream.StepLeg[IO, Sample],
      leg2: Stream.StepLeg[IO, Sample]
    ): Pull[IO, Sample, Unit] = {
      //val z = (leg1.head.isEmpty, leg2.head.isEmpty) match {
      //  case (true, true)   => Pull.done
      //  case (false, false) =>
      //}
      val (chunk, c1, c2) = joinChunks(model1, leg1.head, model2, leg2.head)
      Pull.output(chunk) >> {                                   //keep the merged chunk and append...
        if (c1.isEmpty && c2.isEmpty) leg1.stepLeg.flatMap {
          case Some(l1) => leg2.stepLeg.flatMap {
            case Some(l2) => go(leg1, leg2)                   //resume with next chunk from each stream
            case None =>
              //TODO: need to go until both legs empty
              //  or responsibility of joinChunks?

              Pull.output(l1.head) >> l1.stream.pull.echo   //right stream empty, reconstitute left stream
          }
          case None => leg2.stream.pull.echo                    //left stream empty, keep rest of right stream
        } else if (c1.isEmpty) leg1.stepLeg.flatMap {
          case Some(leg1) => go(leg1, leg2.setHead(c2))         //resume with next chunk of s1 and remainder of c2
          case None => Pull.output(c2) >> leg2.stream.pull.echo //left stream empty, reconstitute right stream
        } else leg2.stepLeg.flatMap {
          case Some(leg2) => go(leg1.setHead(c1), leg2)         //resume with remainder of c1 and next chunk of s2
          case None => Pull.output(c1) >> leg1.stream.pull.echo //right stream empty, reconstitute left stream
        }
      }
    }

    val s1 = data1.samples
    val s2 = data2.samples

    //TODO: allow empty stream
    //  this designed for append
    //val z = {
    //  val a = s1.pull.stepLeg
    //  val b = s2.pull.stepLeg
    //  val q = (a, b).map2 { p =>
    //    ???
    //  }
    //}
    // Start pulling from the Streams
    val samples = s1.pull.stepLeg.flatMap {
      case Some(l1) => s2.pull.stepLeg.flatMap {
        case Some(l2) => go(l1, l2)
        case None => s1.pull.echo //s2 empty
      }
      case None => s2.pull.echo //s1 empty
    }.stream //turn the Pull back into a Stream

    StreamFunction(samples).asRight
  }

  //TODO: who calls this?
  //  CompositeDataset only calls applyToData
  //  Monoid?
  final def combine(ds1: Dataset, ds2: Dataset): Dataset = {
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
    val model = applyToModel(ds1.model, ds2.model).fold(throw _, identity)
    val data = applyToData(
      ds1.model, StreamFunction(ds1.samples),
      ds2.model, StreamFunction(ds2.samples)
    ).fold(throw _, identity)
    new TappedDataset(md, model, data)
  }

  /**
   * Tests whether the domain variables from two models are equivalent.
   *
   * Tests only that the domain variable ids, types, and units match.
   * Note, relational algebra goes by attribute (i.e. column name) only.
   *
   */
  //TODO: util?
  //TODO: convert units?
  final def equivalentDomain(model1: DataType, model2: DataType): Boolean = {
    (model1, model2) match {
      case (Function(d1, _), Function(d2, _)) =>
        val d1s = d1.getScalars
        val d2s = d2.getScalars
        d1s.size == d2s.size &&
          d1s.zip(d2s).forall { pair =>
            (pair._1.id == pair._2.id) &&
              (pair._1.valueType == pair._2.valueType) &&
              (pair._1.units == pair._2.units)
          }
      case (_, _) => true //scalar or tuple, 0-arity
    }
  }
}

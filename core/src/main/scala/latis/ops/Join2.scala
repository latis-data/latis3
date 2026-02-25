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
trait Join2 extends BinaryOperation2 {
  //TODO: replace Join
  //TODO: use for CompositeDataset, needs to use join for model, had been assuming vertical join

  // Returns the new chunk and the remainder of the other two
  def joinChunks(
    model1: DataType,
    c1: Chunk[Sample],
    model2: DataType,
    c2: Chunk[Sample]
  ): (Chunk[Sample], Chunk[Sample], Chunk[Sample])

  def applyToModel(model1: DataType, model2: DataType): Either[LatisException, DataType]

  override def applyToData(
    model1: DataType,
    stream1: Stream[IO, Sample],
    model2: DataType,
    stream2: Stream[IO, Sample],
  ): Either[LatisException, Stream[IO, Sample]] = {

    def go(
      leg1: Option[Stream.StepLeg[IO, Sample]],
      leg2: Option[Stream.StepLeg[IO, Sample]]
    ): Pull[IO, Sample, Unit] = {
      //TODO: do we need Option?

      val chunk1 = leg1.map(_.head).getOrElse(Chunk.empty)
      val chunk2 = leg2.map(_.head).getOrElse(Chunk.empty)
      val (chunk, c1, c2) = joinChunks(model1, chunk1, model2, chunk2)

      //TODO: should we test all empty? or rely on joinChunks?
      //if (chunk.isEmpty && c1.isEmpty && c2.isEmpty) Pull.done //inf loop looking for more on empty s1
      if (chunk.isEmpty) Pull.done
      else Pull.output(chunk) >> { //output joined chunk, recurse with the rest
        if (c1.isEmpty && c2.isEmpty) {
          (leg1.flatTraverse(_.stepLeg), leg2.flatTraverse(_.stepLeg))
            .mapN(go).flatten
        } else if (c1.isEmpty) leg1.flatTraverse(_.stepLeg).flatMap {
          case Some(l1) => go(l1.some, leg2)
          case None     => go(None, leg2)
        } else if (c2.isEmpty) leg2.flatTraverse(_.stepLeg).flatMap {
          case Some(l2) => go(leg1, l2.some)
          case None     => go(leg1, None)
        } else ??? //leftover samples from both streams, valid case?
      }
    }

    // Start pulling from the Streams and recurse
    (stream1.pull.stepLeg, stream2.pull.stepLeg)
      .mapN(go).flatten.stream.asRight
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
    val samples = applyToData(
      ds1.model, ds1.samples,
      ds2.model, ds2.samples
    ).fold(throw _, identity)
    new TappedDataset(md, model, StreamFunction(samples))
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

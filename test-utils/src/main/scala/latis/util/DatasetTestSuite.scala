package latis.util

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers._

import latis.catalog.Catalog
import latis.data.Datum
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.model.DataType
import latis.ops._

abstract class DatasetTestSuite extends AnyFunSuite {

  val catalog: IO[Catalog]

  private def findDataset(id: Identifier): IO[Option[Dataset]] =
    catalog.flatMap(_.findDataset(id))

  def withDataset(dataset: Identifier, ops: List[UnaryOperation] = List.empty)(f: Dataset => Any): Any =
    findDataset(dataset).map {
      case Some(ds) => f(ds.withOperations(ops))
      case None     => fail(s"Dataset not found: ${dataset.asString}")
    }.unsafeRunSync()


  def matchModel(dataset: Identifier, ops: List[UnaryOperation] = List.empty)
                (f: PartialFunction[DataType, Assertion]): Assertion =
    findDataset(dataset).map {
      case Some(ds) => inside(ds.withOperations(ops).model)(f)
      case None => fail(s"Dataset not found: ${dataset.asString}")
    }.unsafeRunSync()


  def matchFirstSample(dataset: Identifier, ops: List[UnaryOperation] = List.empty)
                      (f: PartialFunction[Sample, Assertion]): Assertion =
    (for {
      ods <- findDataset(dataset)
      ds = ods.getOrElse(fail(s"Dataset not found: ${dataset.asString}"))
      samples <- ds
        .withOperations(ops :+ Head())
        .samples.compile.toList
    } yield {
      samples.headOption match {
        case Some(sample) => inside(sample)(f)
        case None => fail("Empty dataset")
      }
    }).unsafeRunSync()


  def equalsFirstSample(dataset: Identifier, ops: List[UnaryOperation] = List.empty)
                       (values: Any*): Assertion =
    (for {
      ods <- findDataset(dataset)
      ds = ods.getOrElse(fail(s"Dataset not found: ${dataset.asString}"))
      samples <- ds
        .withOperations(ops :+ Head())
        .samples.compile.toList
    } yield {
      samples.headOption match {
        case Some(sample) =>
          (sample.domain ++ sample.range).collect {
            case d: Datum => d
            case _: SampledFunction => fail("Equality test on Function not supported")
          }.map(_.value) should be (values)
        case None => fail("Empty dataset")
      }
    }).unsafeRunSync()

}

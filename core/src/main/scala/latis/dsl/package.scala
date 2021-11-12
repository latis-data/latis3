package latis

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.file.Files
import fs2.io.file.Path
import fs2.text

import latis.data.DomainSet
import latis.data.Sample
import latis.dataset.Dataset
import latis.input.DatasetReader
import latis.ops._
import latis.output.CsvEncoder
import latis.output.OutputStreamWriter
import latis.output.TextEncoder
import latis.util.FileUtils
import latis.util.Identifier

package object dsl {

  /**
   * Defines the DSL for the functional algebra
   */
  implicit class DatasetOps(dataset: Dataset) {
    def select(exp: String): Dataset = dataset.withOperation(
      Selection.makeSelection(exp).fold(throw _, identity)
    )
    def project(exp: String): Dataset = dataset.withOperation(
      Projection.fromExpression(exp).fold(throw _, identity)
    )
    def stride(s: Int, ss: Int*): Dataset          = dataset.withOperation(Stride((s +: ss).toIndexedSeq))
    def uncurry(): Dataset                         = dataset.withOperation(Uncurry())
    def curry(n: Int): Dataset                     = dataset.withOperation(Curry(n))
    def groupByVariable(ids: Identifier*): Dataset = dataset.withOperation(GroupByVariable(ids: _*))
    def groupByBin(set: DomainSet, agg: Aggregation = DefaultAggregation()): Dataset =
      dataset.withOperation(GroupByBin(set, agg))
    def substitute(df: Dataset): Dataset           = dataset.withOperation(Substitution(df))
    def compose(df: Dataset): Dataset              = dataset.withOperation(Composition(df))
    def contains(id: Identifier, values: String*): Dataset =
      dataset.withOperation(Contains(id, values: _*))
    def rename(id: Identifier, newId: Identifier): Dataset =
      dataset.withOperation(Rename(id, newId))
    def eval(value: String): Dataset               = dataset.withOperation(Evaluation(value))
    def withReader(reader: DatasetReader): Dataset = dataset.withOperation(ReaderOperation(reader))
    def drop(n: Long): Dataset                     = dataset.withOperation(Drop(n))
    def dropLast(): Dataset                        = dataset.withOperation(DropLast())
    def dropRight(n: Int): Dataset                 = dataset.withOperation(DropRight(n))
    def head(): Dataset                            = dataset.withOperation(Head())
    def first(): Dataset                           = dataset.withOperation(Head())
    def last(): Dataset                            = dataset.withOperation(Last())
    def tail(): Dataset                            = dataset.withOperation(Tail())
    def take(n: Int): Dataset                      = dataset.withOperation(Take(n))
    def takeRight(n: Int): Dataset                 = dataset.withOperation(TakeRight(n))
    def transpose(): Dataset                       = dataset.withOperation(Transpose())

    def filter(predicate: Sample => Boolean): Dataset = dataset.withOperation(Filter(predicate))
    //TODO: map, flataMap, mapRange, but need to know how model changes

    /**
     * Writes a dataset to stdout using the TextEncoder
     * which preserves the functional structure.
     */
    def show(): Unit = new TextEncoder()
      .encode(dataset)
      .map(println)
      .compile
      .drain
      .unsafeRunSync()

    /**
     * Writes the dataset to a file based on its extension.
     */
    def write(file: String): Unit = FileUtils.getExtension(file) match {
      //TODO: factor out extension to encoder mapping, reuse in dap2 service
      case Some("csv") => CsvEncoder()
        .encode(dataset)
        .through(text.utf8.encode)
        .through(Files[IO].writeAll(Path(file)))
        .compile
        .drain
        .unsafeRunSync()
      //case "nc"  => //TODO: need to be able to dynamically load dependendies
      case Some(ext) => throw new RuntimeException(s"Unsupported file extension: $ext")
      case None      => throw new RuntimeException("No file extension.")
    }
  }
}

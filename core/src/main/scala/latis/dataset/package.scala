package latis

import latis.data._
import latis.input.DatasetReader
import latis.ops._
import latis.util.Identifier

package object dataset {

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
    def stride(s: Int, ss: Int*): Dataset = dataset.withOperation(Stride((s +: ss).toIndexedSeq))
    def uncurry(): Dataset = dataset.withOperation(Uncurry())
    def curry(n: Int): Dataset = dataset.withOperation(Curry(n))
    def groupByVariable(ids: Identifier*): Dataset = dataset.withOperation(GroupByVariable(ids: _*))
    def groupByBin(set: DomainSet, agg: Aggregation = DefaultAggregation()): Dataset = dataset.withOperation(GroupByBin(set, agg))
    def substitute(df: Dataset): Dataset = dataset.withOperation(Substitution(df))
    def compose(df: Dataset): Dataset = dataset.withOperation(Composition(df))
    def contains(id: Identifier, values: String*): Dataset = dataset.withOperation(Contains(id, values: _*))
    def rename(id: Identifier, newId: Identifier): Dataset = dataset.withOperation(Rename(id, newId))
    def eval(value: String): Dataset = dataset.withOperation(Evaluation(value))
    def withReader(reader: DatasetReader): Dataset = dataset.withOperation(ReaderOperation(reader))
    def drop(n: Long): Dataset = dataset.withOperation(Drop(n))
    def dropLast(): Dataset = dataset.withOperation(DropLast())
    def dropRight(n: Int): Dataset = dataset.withOperation(DropRight(n))
    def head(): Dataset = dataset.withOperation(Head())
    def first(): Dataset = dataset.withOperation(Head())
    def last(): Dataset = dataset.withOperation(Last())
    def tail(): Dataset = dataset.withOperation(Tail())
    def take(n: Long): Dataset = dataset.withOperation(Take(n))
    def takeRight(n: Int): Dataset = dataset.withOperation(TakeRight(n))

    def filter(predicate: Sample => Boolean): Dataset = dataset.withOperation(Filter(predicate))
    //TODO: map, flataMap, mapRange, but need to know how model changes
  }
}

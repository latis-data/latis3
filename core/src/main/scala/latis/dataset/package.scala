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
    def select(exp: String): Dataset = dataset.withOperation(Selection(exp))
    def project(exp: String): Dataset = dataset.withOperation(Projection(exp))
    def stride(s: Int, ss: Int*): Dataset = dataset.withOperation(Stride((s +: ss).toIndexedSeq))
    def uncurry(): Dataset = dataset.withOperation(Uncurry())
    def curry(n: Int): Dataset = dataset.withOperation(Curry(n))
    def groupByVariable(vars: Identifier*): Dataset = dataset.withOperation(GroupByVariable(vars: _*))
    def groupByBin(set: DomainSet, agg: Aggregation = DefaultAggregation()): Dataset = dataset.withOperation(GroupByBin(set, agg))
    def substitute(df: Dataset): Dataset = dataset.withOperation(Substitution(df))
    def compose(df: Dataset): Dataset = dataset.withOperation(Composition(df))
    def contains(varName: Identifier, values: String*): Dataset = dataset.withOperation(Contains(varName, values: _*))
    def rename(varName: Identifier, newName: Identifier): Dataset = dataset.withOperation(Rename(varName, newName))
    def eval(value: String): Dataset = dataset.withOperation(Evaluation(value))
    def withReader(reader: DatasetReader): Dataset = dataset.withOperation(ReaderOperation(reader))

    def filter(predicate: Sample => Boolean): Dataset = dataset.withOperation(Filter(predicate))
    //TODO: map, flataMap, mapRange, but need to know how model changes
  }
}

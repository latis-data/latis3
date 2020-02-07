package latis

import latis.data.DomainSet
import latis.data.Sample
import latis.ops._

package object dataset {

  /**
   * Defines the DSL for the functional algebra
   */
  implicit class DatasetOps(dataset: Dataset) {
    def select(exp: String): Dataset = dataset.withOperation(Selection(exp))
    def project(exp: String): Dataset = dataset.withOperation(Projection(exp))
    def stride(n: Int): Dataset = dataset.withOperation(Stride(n))
    def stride(stride: Array[Int]): Dataset = dataset.withOperation(Stride(stride))
    def uncurry(): Dataset = dataset.withOperation(Uncurry())
    def curry(n: Int): Dataset = dataset.withOperation(Curry(n))
    def groupByVariable(vars: String*): Dataset = dataset.withOperation(GroupByVariable(vars: _*))
    def groupByBin(set: DomainSet, agg: Aggregation = DefaultAggregation()): Dataset = dataset.withOperation(GroupByBin(set, agg))
    def substitute(df: DatasetFunction): Dataset = dataset.withOperation(Substitution(df))
    def compose(df: DatasetFunction): Dataset = dataset.withOperation(Composition(df))
    def contains(varName: String, values: String*): Dataset = dataset.withOperation(Contains(varName, values))
    def rename(varName: String, newName: String): Dataset = dataset.withOperation(Rename(varName, newName))

    def filter(predicate: Sample => Boolean): Dataset = dataset.withOperation(Filter(predicate))
    //TODO: map, flataMap, mapRange, but need to know how model changes
  }
}

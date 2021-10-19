package latis.ops

import latis.data.StreamFunction
import latis.dataset._


/**
 * A Join is a BinaryOperation that combines two or more Datasets.
 *
 * Unlike arbitrary binary operations, Joins provide lawful behavior
 * for distributing operations to the operands before applying the join.
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
trait Join extends BinaryOperation {

  final def combine(ds1: Dataset, ds2: Dataset): Dataset = {
    val md = ds1.metadata //TODO: combine metadata
    val model = applyToModel(ds1.model, ds2.model).fold(throw _, identity)
    val data = applyToData(StreamFunction(ds1.samples), StreamFunction(ds2.samples)).fold(throw _, identity)
    new TappedDataset(md, model, data)
  }
}

# Array Data Model

## Array as Function

An array can be thought of as a function of index: `i -> a`. Unlike a regular *functional data model* `Function`, an array index is not an explicit part of the data. One motivation for this is filtering. Operating on an array should return an array. The indices of that resulting array should be consecutive whole numbers. With explicit index variables, filtering out samples would require rewriting index values. This violates the lean `Filter` operation which simply applies a predicate to each sample to keep or exclude.

## Index Variable

LaTiS defines an `Index` variable as a subclass of `Scalar`. (Not to be confused with the `IndexDatum` data trait that matches `Integer` data and extracts an `Int` suitable for indexing.) The `Index` variable, only applicable for domain variables, ensures that the model represents the dimensionality of an array dataset.

## Array Dataset Output Representation

As one would expect from an array, most encoded representations of an array dataset do not include the index values. One exception is the `TextEncoder` which preserves the functional form of a dataset. It will generate index values for its output as it traverses the data.

## Index Representation in Samples

Since `Index` variables are not explicitly represented in data, we do not represent index data in a `Sample`. The purpose of a `Sample` is to be a lean data container. It should only be interpreted in the context of the corresponding `Function`'s `DataType`. LaTiS `Encoders`, `Operations`, and other code should be vigilant of special behavior for Index variables.

Note that we have `ArrayFunction`s for efficiently managing array data as a `SampledFunction`. These currently manage the index values explicitly in the `Sample`s. This needs to be revisited.

## Multi-Dimensional Arrays

The domain set of a multi-dimensional array dataset (e.g. `(i, j) -> a`) is necessarily a Cartesian product of the individual dimensions. Contrast this with the representation of multi-dimensional arrays in C-like languages (including Scala: Array[Array[Int]]) which are actually arrays of arrays. Such data would be represented as nested `Function`s in the *functional data model*: `i -> j -> a`. This allows for different length (ragged) nested arrays. Mathematical multi-dimensional arrays (as implemented in Fortran and netCDF) require constant length dimensions. Note that multi-dimensional indices could not be generated (e.g. for the `TextEncoder`) without knowing at least the length of the inner dimensions. (The outer dimension can be unlimited.) Likewise, evaluating such a dataset at a given set of indices requires a Cartesian dataset to enable index math.

## Result of Projection

An array dataset may arise as the result of a projection operation that does not include the domain variable(s). An `Index` variable placeholder is required to preserve ordering and uniqueness of samples when the domain variables are no longer included. 

A similar need arises when no range variable is projected. Since it does not make sense for a function to return nothing (`x -> ()`), the projected domain variable becomes a range variable with an `Index` variable replacing it in the domain to preserve the shape (`_ix -> x`). (Note that this is akin to a coordinate variable in netCDF.)

If a multi-dimensional `Function` is specified to be Cartesian (not yet supported), each non-projected `Scalar` can be replaced with an `Index`. In the general case, without an understanding of the topology, we can't simply make such replacements. Instead, if any domain variable is not projected, we must replace the entire multi-dimensional domain with a single `Index`. In the case where some domain variable are projected (not yet supported), they will be prepended to the range variables.

Note that an `Index` variable replacing a non-projected domain variable must have a new identifier. We use the naming convention where "_i" is prepended to the original identifier. If a domain `Tuple` needs to be replaced with an `Index`, we generate a unique identifier (starting with "_i").

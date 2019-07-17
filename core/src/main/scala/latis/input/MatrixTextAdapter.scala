package latis.input

import latis.data._
import latis.model._

import java.net.URI

/**
 * Read a text file which represents a matrix.
 * Row and column values are 0-based integers.
 * This differs from tabular text data in that columns
 * represent a dimension (i.e. domain variable) and not 
 * a tuple (i.e. range variables).
 * Unlike gridded data, the column dimension varies fastest
 * and the row dimension varies from top to bottom. The same 
 * row/column semantics apply to Image data.
 */
case class MatrixTextAdapter(
  model: DataType, 
  config: TextAdapter.Config = TextAdapter.Config()
) extends TextAdapter(model, config) {
  //TODO: assume (row, column) -> value  model?
  
  /**
   * Get a Stream of records that represent rows of the matrix
   * then memoize the data as a row-major 2D array.
   */
  override def apply(uri: URI): SampledFunction = {
    
    // The values represent a single scalar in the range of the Function.
    val scalar = model match {
      case Function(_, scalar: Scalar) => scalar
      case _ => ??? //invalid model for matrix
    }
    
    // Row-major 2D array of parsed data values
    val values: Array[Array[RangeData]] =
      recordStream(uri)  //stream of rows
        .compile.toVector.unsafeRunSync() //unparsed rows as Vector[String]
        .map(_.split(config.delimiter)    //delimited values unparsed: Vector[Array[String]]
          .map(v => RangeData(scalar.parseValue(v)))  //parsed row: Vector[Array[RangeData]]
        ).toArray                         //convert Vector of rows to Array of rows
    
    ArrayFunction2D(values)
  }
}
  

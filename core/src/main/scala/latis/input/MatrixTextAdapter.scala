package latis.input

import java.net.URI

import latis.data._
import latis.model._
import latis.ops.Operation

/**
 * Reads a text file which represents a matrix.
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
  config: TextAdapter.Config = new TextAdapter.Config()
) extends TextAdapter(model, config) {
  //TODO: assume (row, column) -> value  model?

  /**
   * Gets a Stream of records that represent rows of the matrix
   * then memoizes the data as a row-major 2D array.
   */
  override def getData(uri: URI, ops: Seq[Operation] = Seq.empty): SampledFunction = {

    // The values represent a single scalar in the range of the Function.
    val scalar = model match {
      case Function(_, scalar: Scalar) => scalar
      case _ => ??? //invalid model for matrix
    }

    // Row-major 2D array of parsed data values
    val values: Array[Array[TupleData]] =
      recordStream(uri) //stream of rows
        .compile
        .toList
        .unsafeRunSync() //unparsed rows as List[String]
        .map {
          _.split(config.delimiter) //delimited values unparsed: List[Array[String]]
            .map { v =>
              scalar.parseValue(v) match {
                case Right(d) => TupleData(List(d))
                case Left(_) => TupleData(scalar.fillValue) //TODO: improve API
              }
            } //parsed row: List[Array[TupleData]]
        }
        .toArray //convert List of rows to Array of rows

    ArrayFunction2D(values)
  }
}

//=============================================================================

object MatrixTextAdapter extends AdapterFactory {

  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): TextAdapter =
    new TextAdapter(model, new TextAdapter.Config(config.properties: _*))

}

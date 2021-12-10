package latis

import latis.model.Scalar

package object input {

  /** Adds helper methods to Scalars for use by the netCDF code. */
  implicit class NetcdfScalarOps(scalar: Scalar) {

    /**
     * Gets the name of the NetcdfFile variable for the given Scalar.
     *
     * This uses the "sourceId" metadata property if it exists, otherwise
     * the scalar id. "." characters will be escaped to work with the API.
     */
    def ncName: String = scalar.metadata.getProperty("sourceId")
      .map(_.replace(".", raw"\."))
      .getOrElse(scalar.id.asString)
  }
}

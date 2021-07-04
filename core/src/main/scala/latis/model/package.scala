package latis

package object model {

  implicit def dataTypeOps(dataType: DataType): DataTypeOps =
    new DataTypeOps(dataType)

  implicit def scalarOps(scalar: Scalar): ScalarOps =
    new ScalarOps(scalar)
}

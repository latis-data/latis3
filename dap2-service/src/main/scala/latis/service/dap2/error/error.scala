package latis.service.dap2.error

sealed trait Dap2Error                                 extends Exception
final case class DatasetResolutionFailure(msg: String) extends Dap2Error
final case class ParseFailure(msg: String)             extends Dap2Error
final case class UnknownExtension(msg: String)         extends Dap2Error
final case class UnknownOperation(msg: String)         extends Dap2Error
final case class InvalidOperation(msg: String)         extends Dap2Error

package latis.lambda.error

sealed trait LatisLambdaError extends Throwable
final case class DatasetResolutionFailure(msg: String) extends LatisLambdaError
final case class InvalidOperation(msg: String) extends LatisLambdaError
final case class ParseFailure(msg: String) extends LatisLambdaError
final case class UnknownExtension(msg: String) extends LatisLambdaError

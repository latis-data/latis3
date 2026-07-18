package latis.service.sql

enum SqlServiceError extends Throwable {

  /** TODO */
  case DatasetResolutionFailure(msg: String)

  /** TODO */
  case ParseError(msg: String)
}

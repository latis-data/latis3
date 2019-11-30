package latis.util

case class LatisException(
  message: String = "Unknown LaTiS error",
  cause: Throwable = null
) extends Exception(message, cause)

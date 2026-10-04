package latis.util

/** Base class of a sealed set of exceptions. */
sealed abstract class LatisException(
  val message: String,
  val cause: Throwable
) extends Exception(message, cause)

/** Constructors for backwards compatibility. */
object LatisException {

  def apply(message: String, cause: Throwable): LatisException =
    LatisError(message, cause)

  def apply(message: String): LatisException =
    LatisError(message, null)

  def apply(t: Throwable): LatisException = LatisError(t.getMessage, t)

  def apply(): LatisException = LatisError("Unknown LaTiS error", null)
}

/** General LaTiS Error. */
final case class LatisError(
  override val message: String = "Unknown LaTiS error",
  override val cause: Throwable = null
) extends LatisException(message, cause)

object LatisError {
  def apply(t: Throwable): LatisError = LatisError(t.getMessage, t)
}

/** Exception that should not happen by design. */
final case class LatisBug(
  override val message: String = "Unknown LaTiS bug",
  override val cause: Throwable = null
) extends LatisException(message, cause)

object LatisBug {
  def apply(t: Throwable): LatisBug = LatisBug(t.getMessage, t)
}

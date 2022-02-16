package latis.util

/** Defines a stub that service projects will replace via sbt-buildinfo. */
object BuildInfo {
  val toMap: Map[String, scala.Any] = Map[String, scala.Any](
    "service" -> "LaTiS",
    "version" -> "3"
  )
}

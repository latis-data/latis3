package latis.util

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * A wrapper around a Lightbend Config object that provides convenient
 * access methods.
 */
object LatisConfig {

  lazy val config: Config =
    ConfigFactory.load()

  private def ifExists[A](path: String)(f: String => A): Option[A] =
    if (config.hasPath(path)) Option(f(path)) else None

  def getInt(path: String): Option[Int] =
    ifExists(path)(config.getInt)

  def getString(path: String): Option[String] =
    ifExists(path)(config.getString)

  def get(path: String): Option[String] =
    getString(path)

  def getOrElse(path: String, default: String): String =
    getString(path).getOrElse(default)

  def getOrElse(path: String, default: Int): Int =
    getInt(path).getOrElse(default)
}

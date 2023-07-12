package latis.server

import java.net.URL

import cats.syntax.all.*
import com.comcast.ip4s.Port
import fs2.io.file.Path
import pureconfig.ConfigCursor
import pureconfig.ConfigReader
import pureconfig.error.ConvertFailure
import pureconfig.error.UserValidationFailed
import pureconfig.module.ip4s.*

given ConfigReader[Path] =
  ConfigReader.fromString(path => Right(Path(path)))

final case class CatalogConf(
  dir: Path,
  validate: Boolean
)

object CatalogConf {
  given ConfigReader[CatalogConf] =
    ConfigReader.forProduct2("dir", "validate")(CatalogConf(_, _))
}

final case class ServerConf(
  port: Port,
  prefix: String
)

object ServerConf {
  given ConfigReader[ServerConf] =
    ConfigReader.forProduct2("port", "prefix")(ServerConf(_, _))
}

final case class ServiceConf(
  services: List[ServiceSpec]
)

object ServiceConf {
  given ConfigReader[ServiceConf] =
    ConfigReader.forProduct1("services")(ServiceConf(_))
}

sealed trait ServiceSpec {
  val prefix: String
  val clss: String
}

object ServiceSpec {
  given ConfigReader[ServiceSpec] with {
    def from(cur: ConfigCursor): ConfigReader.Result[ServiceSpec] =
      cur.asObjectCursor.flatMap(_.atKey("type")).flatMap { atType =>
        atType.asString.flatMap {
          case "class" => ConfigReader[ClassPathServiceSpec].from(cur)
          case "jar" => ConfigReader[JarServiceSpec].from(cur)
          case x => ConfigReader.Result.fail(
            ConvertFailure(
              UserValidationFailed(s"Unsupported service specification: \"$x\""),
              atType
            )
          )
        }
      }
  }
}

final case class JarServiceSpec(
  path: URL,
  prefix: String,
  clss: String
) extends ServiceSpec

object JarServiceSpec {
  given ConfigReader[JarServiceSpec] =
    ConfigReader.forProduct3("path", "prefix", "class")(JarServiceSpec.apply)
}

final case class ClassPathServiceSpec(
  prefix: String,
  clss: String
) extends ServiceSpec

object ClassPathServiceSpec {
  given ConfigReader[ClassPathServiceSpec] =
    ConfigReader.forProduct2("prefix", "class")(ClassPathServiceSpec(_, _))
}

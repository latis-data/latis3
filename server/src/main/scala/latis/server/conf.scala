package latis.server

import java.net.URL

import com.comcast.ip4s.Port
import fs2.io.file.Path
import pureconfig.CamelCase
import pureconfig.ConfigFieldMapping
import pureconfig.KebabCase
import pureconfig.generic.CoproductHint
import pureconfig.generic.FieldCoproductHint
import pureconfig.generic.ProductHint

final case class CatalogConf(
  dir: Path,
  validate: Boolean
)

final case class ServerConf(
  port: Port,
  prefix: String
)

final case class ServiceConf(
  services: List[ServiceSpec]
)

sealed trait ServiceSpec {
  val prefix: String
  val clss: String
}

final case class JarServiceSpec(
  path: URL,
  prefix: String,
  clss: String
) extends ServiceSpec

final case class ClassPathServiceSpec(
  prefix: String,
  clss: String
) extends ServiceSpec

object ServiceSpec {
  implicit val jssHint: ProductHint[JarServiceSpec] =
    ProductHint(
      ConfigFieldMapping(CamelCase, KebabCase).withOverrides(
        "clss" -> "class"
      )
    )

  implicit val cssHint: ProductHint[ClassPathServiceSpec] =
    ProductHint(
      ConfigFieldMapping(CamelCase, KebabCase).withOverrides(
        "clss" -> "class"
      )
    )

  implicit val coproductHint: CoproductHint[ServiceSpec] =
    new FieldCoproductHint[ServiceSpec]("type") {
      override def fieldValue(name: String): String = name match {
        case "JarServiceSpec"       => "jar"
        case "ClassPathServiceSpec" => "class"
      }
    }
}

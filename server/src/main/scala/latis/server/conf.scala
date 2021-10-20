package latis.server

import java.net.URL
import java.nio.file.Path

import com.comcast.ip4s.Port
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
  mapping: String
)

final case class ServiceConf(
  services: List[ServiceSpec]
)

sealed trait ServiceSpec {
  val mapping: String
  val clss: String
}

final case class JarServiceSpec(
  path: URL,
  mapping: String,
  clss: String
) extends ServiceSpec

final case class ClassPathServiceSpec(
  mapping: String,
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

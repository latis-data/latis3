package latis.server

import java.net.URL

import pureconfig.CamelCase
import pureconfig.ConfigFieldMapping
import pureconfig.KebabCase
import pureconfig.generic.CoproductHint
import pureconfig.generic.FieldCoproductHint
import pureconfig.generic.ProductHint

final case class ServerConf(
  port: Int,
  mapping: String
)

final case class ServiceConf(
  services: List[ServiceSpec]
)

sealed trait ServiceSpec {
  val mapping: String
  val clss: String
}

final case class MavenServiceSpec(
  name: String,
  version: String,
  mapping: String,
  clss: String
) extends ServiceSpec

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
  implicit val mssHint: ProductHint[MavenServiceSpec] =
    ProductHint(
      ConfigFieldMapping(CamelCase, KebabCase).withOverrides(
        "clss" -> "class"
      )
    )

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
        case "MavenServiceSpec"     => "maven"
        case "JarServiceSpec"       => "jar"
        case "ClassPathServiceSpec" => "class"
      }
    }
}

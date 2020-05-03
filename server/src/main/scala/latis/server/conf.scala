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

final case class DependencyConf(
  dependencies: List[DependencySpec]
)

sealed trait DependencySpec

final case class JarDependencySpec(url: URL) extends DependencySpec

final case class MavenDependencySpec(
  group: String,
  artifact: String,
  version: String
) extends DependencySpec

object DependencySpec {
  implicit val coproductHint: CoproductHint[DependencySpec] =
    new FieldCoproductHint[DependencySpec]("type") {
      override def fieldValue(name: String): String = name match {
        case "MavenDependencySpec" => "maven"
        case "JarDependencySpec"   => "jar"
      }
    }
}

final case class RepositoryConf(
  repositories: List[URL]
)

final case class ServiceConf(
  services: List[ServiceSpec]
)

final case class ServiceSpec(
  mapping: String,
  clss: String
)

object ServiceSpec {
  implicit val ssHint: ProductHint[ServiceSpec] = ProductHint(
    ConfigFieldMapping(CamelCase, KebabCase).withOverrides("clss" -> "class")
  )
}

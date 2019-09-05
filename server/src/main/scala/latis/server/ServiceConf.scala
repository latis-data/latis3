package latis.server

import pureconfig.CamelCase
import pureconfig.ConfigFieldMapping
import pureconfig.KebabCase
import pureconfig.generic.ProductHint

final case class ServiceConf(
  services: List[ServiceSpec]
)

final case class ServiceSpec(
  name: String,
  version: String,
  mapping: String,
  clss: String
)

object ServiceSpec {
  implicit val hint: ProductHint[ServiceSpec] =
    ProductHint(
      ConfigFieldMapping(CamelCase, KebabCase).withOverrides(
        "clss" -> "class"
      )
    )
}

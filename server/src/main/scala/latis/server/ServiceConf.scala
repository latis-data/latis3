package latis.server

final case class ServiceConf(
  services: List[ServiceSpec]
)

final case class ServiceSpec(
  name: String,
  version: String,
  mapping: String,
  clss: String
)

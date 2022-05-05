package latis.server

case class ServiceInfo(service: String, version: Option[String], latisVersion: Option[String], buildTime: Option[String])

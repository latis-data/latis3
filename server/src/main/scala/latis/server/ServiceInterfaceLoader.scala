package latis.server

import scala.reflect.runtime.{ universe => ru }

import cats.effect.IO
import cats.implicits._

/** TODO */
final class ServiceInterfaceLoader(
  conf: ServiceConf,
  loader: ClassLoader
) {

  /**
   * Load service interfaces described in the service interface
   * configuration.
   */
  val loadServices: IO[List[(ServiceSpec, ServiceInterface)]] =
    conf.services.traverse { spec =>
      loadService(spec).map((spec, _))
    }

  private def loadService(spec: ServiceSpec): IO[ServiceInterface] =
    IO {
      val constructor = {
        val m = ru.runtimeMirror(loader)
        val clss = m.staticClass(spec.clss)
        val cm = m.reflectClass(clss)
        val ctor = clss.toType.decl(ru.termNames.CONSTRUCTOR).asMethod
        cm.reflectConstructor(ctor)
      }
      constructor().asInstanceOf[ServiceInterface]
    }
}

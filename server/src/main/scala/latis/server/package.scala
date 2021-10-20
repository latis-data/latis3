package latis

import com.comcast.ip4s.Port
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

package object server {

  implicit val portReader: ConfigReader[Port] =
    ConfigReader[Int].emap { int =>
      Port.fromInt(int).toRight(
        CannotConvert(int.toString, "Port", "Invalid port number")
      )
    }
}

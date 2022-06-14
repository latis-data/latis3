package latis

import fs2.io.file.Path
import pureconfig.ConfigReader

package object server {

  implicit val fs2PathReader: ConfigReader[Path] =
    ConfigReader.fromString(path => Right(Path(path)))
}

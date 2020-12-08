package latis.fits

import latis.util.ConfigLike

sealed trait Header extends ConfigLike {}

case class BinaryTableHeader(properties: (String, String)*) extends Header {}

case class AsciiTableHeader(properties: (String, String)*) extends Header {}

case class ImageHeader(properties: (String, String)*) extends Header {}

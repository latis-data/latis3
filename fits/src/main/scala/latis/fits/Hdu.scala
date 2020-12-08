package latis.fits

sealed trait Hdu {}

case class BinaryTableHdu(header: BinaryTableHeader, data: BinaryTableData) extends Hdu {}

case class AsciiTableHdu(header: AsciiTableHeader, data: AsciiTableData) extends Hdu {}

case class ImageHdu(header: ImageHeader, data: ImageData) extends Hdu {}

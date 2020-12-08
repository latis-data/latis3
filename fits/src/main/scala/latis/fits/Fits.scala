package latis.fits

import cats.syntax.all._

import latis.util.LatisException

case class Fits private (hdus: List[Hdu]) {

}

object Fits {
  def fromHdus(hdus: List[Hdu]): Either[LatisException, Fits] = hdus match {
    case l@(_:ImageHdu :: _) => Fits(l).asRight
    case _ => Left(LatisException("A FITS file must be a non-empty list of" +
      " HDUs, starting with an image HDU."))
  }
}

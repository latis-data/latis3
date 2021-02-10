package latis.fits

import cats.data.NonEmptyList

case class Fits private (hdus: NonEmptyList[Hdu])

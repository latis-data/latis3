package latis.input

import java.net.URI

import latis.dataset.Dataset
import latis.input.fdml.FdmlReader
import latis.ops._
import latis.output.TextWriter

object TestNetcdfAdapter extends App {

  val fdml =
    //"/Users/lindholm/git/latis3-lisird/fdml/lasp_vsop87_1au_correction_PT1M.fdml"
    //"/Users/lindholm/git/latis3-lisird/fdml/lyman_alpha_model_ssi.fdml"
    //"/Users/lindholm/git/latis3-lisird/fdml/omi_ssi.fdml"
    "/Users/lindholm/git/latis3-lisird/fdml/nrl2_ssi_P1Y.fdml"

  val uri: URI = new URI(fdml)
  val ds: Dataset = FdmlReader.read(uri, validate = false)
    .select("wavelength = 115.5")
    .select("time >= 2000")
    .take(10)
  TextWriter().write(ds)
}

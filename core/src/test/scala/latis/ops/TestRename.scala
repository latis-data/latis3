package latis.ops

import latis.model._
import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import latis.data._

class TestRename extends JUnitSuite {

  @Test
  def model() = {
    val origModel = Function(
      Scalar("band"),
      Function(
        Tuple(
          Scalar("ix"), Scalar("iy")
        ),
        Scalar("radiance")
      )
    )
    
    val model = Rename("ix", "lon").applyToModel(origModel)
    println(model)
    
  }
}

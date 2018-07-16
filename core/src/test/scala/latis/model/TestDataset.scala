package latis.model

import org.junit._
import org.junit.Assert._
import latis.metadata.Metadata

class TestDataset {
  
  val testds = Dataset(
    Metadata("id" -> "testds", "history" -> "none"),
    Function(Metadata("id" -> "f"), Scalar("a"), Scalar("b")),
    ???
  )
}
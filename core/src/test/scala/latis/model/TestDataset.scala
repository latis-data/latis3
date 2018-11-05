package latis.model

import org.junit._
import org.junit.Assert._
import latis.metadata.Metadata
import latis.data._
import latis.output._
import cats.effect._
import fs2._

object TestDataset extends App {
  
  val testds = {
    val md = Metadata("id" -> "testds", "history" -> "none")
    val model = Function(
      Scalar("x"),
      Scalar("a")
    )
    val xs: Array[Any] = Array(0,1,2)
    val as: Array[Any] = Array(1,2,3)
    val data = IndexedFunction1D(xs, as)
    
    Dataset(md, model, data)
  }

  
  //Writer.writeP(testds)
  
}
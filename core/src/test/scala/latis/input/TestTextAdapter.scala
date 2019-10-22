package latis.input

import latis.metadata.Metadata
import latis.model._
import latis.output.TextWriter

import java.net.URI

import org.junit.Test
import org.scalatest.junit.JUnitSuite

class TestTextAdapter extends JUnitSuite {
  
  //@Test
  def test = {
    val reader = new AdaptedDatasetReader {
      def uri: URI = new URI(s"file:${System.getProperty("user.home")}/git/latis3/core/src/test/resources/data/data.txt")
      def model: DataType = Function(
          Scalar(Metadata("id" -> "a", "type" -> "short")),
          Tuple(
            Scalar(Metadata("id" -> "b", "type" -> "int")), 
            Scalar(Metadata("id" -> "c", "type" -> "float")), 
            Scalar(Metadata("id" -> "d", "type" -> "string"))
          )
      )
      val config = new TextAdapter.Config()
      def adapter = new TextAdapter(model, config)
    }
    
    val ds = reader.getDataset
    TextWriter().write(ds)
  }
}
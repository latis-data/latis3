package latis.input

import latis.metadata.Metadata
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.output.Writer

import java.net.URI

import org.junit.Test

class TestTextAdapter {
  
  //@Test
  def test = {
    val source = new AdaptedDatasetSource {
      def uri: URI = new URI(s"file:${System.getProperty("user.home")}/git/latis3/core/src/test/resources/data/data.txt")
      def model: DataType = Function(
          Scalar(Metadata("id" -> "a", "type" -> "string")), 
          Scalar(Metadata("id" -> "b", "type" -> "string"))
      )
      val config = TextAdapter.Config()
      def adapter = new TextAdapter(config, model)
    }
    
    val ds = source.getDataset()
    Writer.write(ds)
  }
}
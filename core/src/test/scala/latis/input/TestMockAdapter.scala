package latis.input

import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.output.Writer

import java.net.URI

import org.junit.Test

class TestMockAdapter {
  
  //@Test
  def mock = {
    val source = new AdaptedDatasetSource {
      def uri: URI = new URI("mock")
      def model: DataType = Function(Scalar("a"), Scalar("b"))
      def adapter = new MockAdapter()
    }
    
    val ds = source.getDataset()
    Writer.write(ds)
  }
}

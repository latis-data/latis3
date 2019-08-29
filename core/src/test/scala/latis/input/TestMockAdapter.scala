package latis.input

import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.output.Writer

import java.net.URI

import org.junit.Test
import org.scalatest.junit.JUnitSuite

class TestMockAdapter extends JUnitSuite {
  
//  //@Test
//  def mock = {
//    val reader = new AdaptedDatasetReader {
//      def uri: URI = new URI("mock")
//      def model: DataType = Function(Scalar("a"), Scalar("b"))
//      def adapter = new MockAdapter()
//    }
//    
//    val ds = reader.getDataset
//    //Writer.write(ds)
//  }
}

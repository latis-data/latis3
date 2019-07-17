package latis.input

import latis.metadata.Metadata
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.model.Tuple
import latis.output.TextWriter

import java.net.URI
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class TestMatrixAdapter extends JUnitSuite {
  
  //@Test
  def test = {
    val reader = new AdaptedDatasetReader {
      def uri: URI = new URI(s"file:/data/hysics/des_veg_cloud/img1000.txt")
      def model: DataType = Function(
          Tuple(
            Scalar(Metadata("id" -> "row", "type" -> "int")), 
            Scalar(Metadata("id" -> "column", "type" -> "int"))
          ),
          Scalar(Metadata("id" -> "v", "type" -> "double"))
      )
      val config = TextAdapter.Config(("delimiter",","))
      
      def adapter = new MatrixTextAdapter(model, config)
    }
    
    val ds = reader.getDataset
    TextWriter().write(ds)
  }
}
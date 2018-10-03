package latis.input

import latis.metadata.Metadata
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.model.Tuple
import latis.output.Writer

import java.net.URI

class TestMatrixAdapter {
  
  //@Test
  def test = {
    val source = new AdaptedDatasetSource {
      def uri: URI = new URI(s"file:/data/hysics/des_veg_cloud/img1000.txt")
      def model: DataType = Function(
          Tuple(
            Scalar(Metadata("id" -> "row", "type" -> "int")), 
            Scalar(Metadata("id" -> "column", "type" -> "int"))
          ),
          Scalar(Metadata("id" -> "v", "type" -> "double"))
      )
      val config = TextAdapter.Config(delimiter=",")
      def adapter = new MatrixTextAdapter(config, model)
    }
    
    val ds = source.getDataset()
    Writer.write(ds)
  }
}
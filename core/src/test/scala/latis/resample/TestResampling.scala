package latis.resample

import latis.data._
import latis.metadata._
import latis.model._

import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import latis.output.TextWriter

class TestResampling extends JUnitSuite {
  
  @Test
  def bin_resampling() = {
    // Define original Function
    val (onx, ony) = (50, 50)
    //val set = LinearSet2D(0.2, 1.0, onx, 0.2, 1.0, ony)
    val set = new RegularSet2D(
      RegularSet1D(1.0, 0.2, onx),
      RegularSet1D(1.0, 0.2, ony)
    )
    val values = for {
      ix <- (0 until onx)
      iy <- (0 until ony)
    } yield RangeData(1.0 * ix * iy)
    val origSF = SetFunction(set, values)
    //origSF.samples foreach println
    
    // Define regular grid to resample onto
    val (nx, ny) = (3,4)
    val domainSet = BinSet2D(
      BinSet1D(2, 1, nx),
      BinSet1D(2, 1, ny)
    )
    //domainSet.elements foreach println
    
    //val data = origSF.resample(domainSet, BinResampling())
    val data = BinResampling().resample(origSF, domainSet)
    
    val model = Function(
      Tuple(
        Scalar(Metadata("id" -> "x", "type" -> "double")),
        Scalar(Metadata("id" -> "y", "type" -> "double"))
      ),
      Scalar(Metadata("id" -> "v", "type" -> "double"))
    )
    
    val md = Metadata("bin_resampling")
    
    val ds = Dataset(md, model, data)
    //TextWriter().write(ds)
    
    val samples = data.unsafeForce.samples
    assertEquals(12, samples.length)
    // match first sample
    samples.head match {
      case Sample(DomainData(Real(x), Real(y)), RangeData(Real(v))) =>
        assertEquals(2.0, x, 0)
        assertEquals(2.0, y, 0)
        assertEquals(25.0, v, 0)
    }
    // match last sample
    samples.last match {
      case Sample(DomainData(Real(x), Real(y)), RangeData(Real(v))) =>
        assertEquals(4.0, x, 0)
        assertEquals(5.0, y, 0)
        assertEquals(300.0, v, 0)
    }
  }
}
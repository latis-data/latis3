package latis.ops

import latis.model._
import org.junit._
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import latis.data._
import latis.metadata.Metadata
import latis.output.TextWriter

class TestSubstitution extends JUnitSuite {

  //@Test
  def model() = {
    val m1 = Function(
      Scalar("band"),
      Function(
        Tuple(
          Scalar("ix"), Scalar("iy")
        ),
        Scalar("radiance")
      )
    )
    val ds1 = {
      val bs = Vector(1,2,3)
      val samples = for {
        b <- bs
        r = Array.tabulate(2,4)((i,j) => RangeData(i * j + b))
      } yield Sample(DomainData(b), RangeData(ArrayFunction2D(r)))
      val data = SeqFunction(samples)
      Dataset(Metadata(""), m1, data)
    }
    
    
//    val m2 = Function(Scalar("ix"), Scalar("x"))
    val m2 = Function(
      Tuple(
        Scalar("ix"), Scalar("iy")
      ),
      //Scalar("z")
      Tuple(
        Scalar("lon"), Scalar("lat")
      )
    )

    val ds2 = {
      val arr = Array.tabulate(2,4)((i,j) => RangeData(i*10 - 100, j*10 + 10))
      val data = ArrayFunction2D(arr)
      Dataset(Metadata(""), m2, data)
    }
    
    //TextWriter(System.out).write(ds1)
    //TextWriter(System.out).write(ds2)
    val ds3 = Substitution()(ds1, ds2)
    TextWriter(System.out).write(ds3)
    
//    
//    println(m1)
//    val model = Substitution().applyToModel(m1, m2)
//    println(model)
    
  }
}

package latis.input

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import ucar.ma2.Range
import ucar.ma2.Section

import latis.metadata.Metadata
import latis.model._
import latis.ops.Selection
import latis.util.LatisException

class NetcdfAdapterSpec extends FlatSpec {

  "A section" should "be strided" in {
    val section            = new Section("0:9:3, 1:4:2")
    val stride: Array[Int] = Array(2, 2)
    val s                  = NetcdfAdapter.applyStride(section: Section, stride: Array[Int])
    s.toString should be("Right(0:6:6,1:1:4)")
  }

  "A range with a stride" should "make a consistent last value" in {
    val range = new Range(1, 4, 2) // 1:3:2
    range.last should be(3)
  }

  "A NetcdfAdapter selection operation" should "support LT selection" in {
    val expectedSection = new Section("0:1")
    NetcdfAdapter.applySelection(new Section("0:2"), model, Selection("time < 8.5")) should be(
      Right(expectedSection)
    )
    NetcdfAdapter.applySelection(new Section("0:2"), model, Selection("time < 9")) should be(
      Right(expectedSection)
    )
  }

  "A NetcdfAdapter selection operation" should "support GT selection" in {
    val expectedSection = new Section("1:2")
    NetcdfAdapter.applySelection(new Section("0:2"), model, Selection("time > 7.5")) should be(
      Right(expectedSection)
    )
    NetcdfAdapter.applySelection(new Section("0:2"), model, Selection("time > 7")) should be(
      Right(expectedSection)
    )
  }

  "A NetcdfAdapter selection operation" should "support LE selection" in {
    val expectedSection = new Section("0:1")
    NetcdfAdapter.applySelection(new Section("0:2"), model, Selection("time <= 8.5")) should be(
      Right(expectedSection)
    )
    NetcdfAdapter.applySelection(new Section("0:2"), model, Selection("time <= 8")) should be(
      Right(expectedSection)
    )
  }

  "A NetcdfAdapter selection operation" should "support GE selection" in {
    val expectedSection = new Section("1:2")
    NetcdfAdapter.applySelection(new Section("0:2"), model, Selection("time >= 7.5")) should be(
      Right(expectedSection)
    )
    NetcdfAdapter.applySelection(new Section("0:2"), model, Selection("time >= 8")) should be(
      Right(expectedSection)
    )
  }

  "A NetcdfAdapter selection operation" should "handle invalid selections gracefully" in {
    NetcdfAdapter.applySelection(new Section("0:2"), model, Selection("time < 6")) should be(
      Left(LatisException("Selection value is outside of data range"))
    )
  }

  private val model = Function(
    Scalar(Metadata("id" -> "time", "type" -> "int", "cadence" -> "1", "start" -> "7")),
    Scalar(Metadata("id" -> "flux", "type" -> "double"))
  )
}

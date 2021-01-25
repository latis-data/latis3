package latis.input

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import ucar.ma2.Section
import ucar.ma2.{Range => URange}

import latis.metadata.Metadata
import latis.model._
import latis.ops.Selection

class NetcdfAdapterSpec extends AnyFlatSpec {

  private val simple1dModel = Function(
    Scalar(Metadata("id" -> "time", "type" -> "int", "cadence" -> "1", "start" -> "7")),
    Scalar(Metadata("id" -> "flux", "type" -> "double"))
  )
  private val simple1dSections = makeSections("0:9; 0:9")

  private val simple2dModel = Function(
    Tuple(
      Scalar(Metadata("id" -> "time", "type" -> "int", "cadence" -> "1", "start" -> "7")),
      Scalar(Metadata("id" -> "wavelength", "type" -> "double", "cadence" -> "0.1", "start" -> "5.0")),
    ),
    Scalar(Metadata("id" -> "flux", "type" -> "double"))
  )
  private val simple2dSections = makeSections("0:9; 0:4; 0:9, 0:4")

  private val sdoDiodesModel = Function(
    Tuple(
      Scalar(Metadata("id" -> "time", "type" -> "int", "cadence" -> "1", "start" -> "7")),
      Scalar(Metadata("id" -> "wavelength", "type" -> "double")),
    ),
    Tuple(
      Scalar(Metadata("id" -> "flux", "type" -> "double")),
      Scalar(Metadata("id" -> "error", "type" -> "double")),
    )
  )
  private val sdoDiodesSections = makeSections("0, 0:3760; 0:5; 0, 0:3760, 0:5; 0, 0:3760, 0:5")

  private val timeModel = Function(
    latis.time.Time(Metadata(
      "id" -> "time",
      "class" -> "latis.time.Time",
      "units" -> "yyyyMMdd",
      "type" -> "string",
      "cadence" -> "86400000", // ms in a day
      "start" -> s"${86400000L * 365}"
    )),
    Scalar(Metadata("id" -> "flux", "type" -> "double"))
  )
  private val timeSections = makeSections("0:31; 0:31")

  private def makeSections(s: String): List[Section] =
    s.split(';').toList.map(new Section(_))

  //-- Tests -----------------------------------------------------------------//

  "A Stride operation" should "apply a stride to a 1D model" in {
    val stride: Array[Int] = Array(2)
    NetcdfAdapter.applyStride(simple1dSections, simple1dModel, stride: Array[Int]) should be(
      Right(makeSections("0:8:2; 0:8:2"))
    )
  }

  it should "apply a stride to a 2D model" in {
    val stride: Array[Int] = Array(2, 3)
    NetcdfAdapter.applyStride(simple2dSections, simple2dModel, stride: Array[Int]) should be(
      // 0:9; 0:4; 0:9, 0:4
      Right(makeSections("0:8:2; 0:3:3; 0:8:2, 0:3:3"))
    )
  }

  "A NetcdfAdapter selection operation" should "support < selection" in {
    simpleSelectTest("time < 8.5", new URange(0, 1))
    simpleSelectTest("time < 9", new URange(0, 1))
    simpleSelectTest("time < 9001", new URange(0, 9))
    simpleSelectTest("time < 0", URange.EMPTY)
  }

  it should "support > selection" in {
    simpleSelectTest("time > 7.5", new URange(1, 9))
    simpleSelectTest("time > 7", new URange(1, 9))
    simpleSelectTest("time > 0", new URange(0, 9))
    simpleSelectTest("time > 9000", URange.EMPTY)
  }

  it should "support <= selection" in {
    simpleSelectTest("time <= 8.5", new URange(0, 1))
    simpleSelectTest("time <= 8", new URange(0, 1))
    simpleSelectTest("time <= 9001", new URange(0, 9))
    simpleSelectTest("time <= 0", URange.EMPTY)
  }

  it should "support >= selection" in {
    simpleSelectTest("time >= 7.5", new URange(1, 9))
    simpleSelectTest("time >= 8", new URange(1, 9))
    simpleSelectTest("time >= 0", new URange(0, 9))
    simpleSelectTest("time >= 9001", URange.EMPTY)
  }

  it should "support = and == selections" in {
    simpleSelectTest("time = 8", new URange(1, 1))
    simpleSelectTest("time == 8", new URange(1, 1))
    simpleSelectTest("time = 8.01", URange.EMPTY)
    simpleSelectTest("time == 8.01", URange.EMPTY)
    simpleSelectTest("time = 0", URange.EMPTY)
    simpleSelectTest("time == 0", URange.EMPTY)
  }

  it should "support ~ selections" in {
    simpleSelectTest("time ~ 8", new URange(1, 1))
    simpleSelectTest("time ~ 8.4", new URange(1, 1))
    simpleSelectTest("time ~ 8.7", new URange(2, 2))
    simpleSelectTest("time ~ 0", new URange(0, 0))
    simpleSelectTest("time ~ 9001", new URange(9, 9))
  }

  it should "apply selections on a 1D model" in {
    // simple1dSections = 0:9; 0:9
    simpleSelectTest("time > 10", new URange(4, 9))
  }

  it should "apply selections on a 2D model" in {
    // simple2dSections = 0:9; 0:4; 0:9, 0:4
    NetcdfAdapter.applySelection(
      simple2dSections,
      simple2dModel,
      Selection.makeSelection("wavelength == 5.2").fold(throw _, identity)
    ) should be(
      Right(makeSections("0:9; 2:2; 0:9, 2:2"))
    )
  }

  it should "apply selections on a model like SDO EVE diodes l3" in {
    // sdoDiodesSections = 0, 0:3760; 0:5; 0, 0:3760, 0:5; 0, 0:3760, 0:5
    NetcdfAdapter.applySelection(
      sdoDiodesSections,
      sdoDiodesModel,
      Selection.makeSelection("time <= 100").fold(throw _, identity)
    ) should be(
      Right(makeSections("0, 0:93; 0:5; 0, 0:93, 0:5; 0, 0:93, 0:5"))
    )
  }

  it should "support selections that use formatted time strings" in {
    NetcdfAdapter.applySelection(
      timeSections,
      timeModel,
      Selection.makeSelection("time > 19710102").fold(throw _, identity)
    )
      .flatMap(
        NetcdfAdapter.applySelection(
          _,
          timeModel,
          Selection.makeSelection("time <= 19710115T123000.000").fold(throw _, identity)
        )
      ) should be(Right(makeSections("2:14; 2:14")))
  }

  def simpleSelectTest(selection: String, expectedRange: URange): Unit =
    NetcdfAdapter.applySelection(
      simple1dSections,
      simple1dModel,
      Selection.makeSelection(selection).fold(throw _, identity)
    ) should be(
      Right(List.fill(2)(new Section(expectedRange)))
    )
}

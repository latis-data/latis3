package latis.input

import java.net.URI

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.Data.IntValue
import latis.data.Data.StringValue
import latis.data.MemoizedFunction
import latis.data.Sample
import latis.data.SampledFunction
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar

final class GranuleListAppendAdapterSpec extends FlatSpec {

  "A granule list append adapter" should
    "create a dataset from appended granules" in {

      val glaSamples = new GranuleListAppendAdapter(granuleList, template)
        .getData(List.empty)
        .samples
        .compile
        .toList
        .unsafeRunSync

      val gSamples = (g1.samples ++ g2.samples ++ g3.samples)
        .compile
        .toList
        .unsafeRunSync

      glaSamples should equal (gSamples)
  }

  private val granules: List[List[Sample]] = List(
    // Contents of first granule
    List(
      Sample(List(StringValue("2020-01-01")), List(IntValue(1))),
      Sample(List(StringValue("2020-01-02")), List(IntValue(2))),
      Sample(List(StringValue("2020-01-03")), List(IntValue(3)))
    ),
    // Contents of second granule
    List(
      Sample(List(StringValue("2020-01-04")), List(IntValue(4))),
      Sample(List(StringValue("2020-01-05")), List(IntValue(5))),
      Sample(List(StringValue("2020-01-06")), List(IntValue(6)))
    ),
    // Contents of third granule
    List(
      Sample(List(StringValue("2020-01-07")), List(IntValue(7))),
      Sample(List(StringValue("2020-01-08")), List(IntValue(8))),
      Sample(List(StringValue("2020-01-09")), List(IntValue(9)))
    )
  )

  // Dataset for first granule
  private val g1: Dataset = {
    val md: Metadata = Metadata("g1")

    val model: DataType = Function(
      Scalar(
        Metadata(
          "id" -> "time",
          "type" -> "string",
          "class" -> "latis.time.Time",
          "units" -> "yyyy-MM-dd"
        )
      ),
      Scalar(
        Metadata(
          "id" -> "value",
          "type" -> "int"
        )
      )
    )

    val samples: MemoizedFunction = SampledFunction(granules(0))

    new MemoizedDataset(md, model, samples)
  }

  // Dataset for second granule
  private val g2: Dataset = {
    val md: Metadata = Metadata("g2")

    val model: DataType = Function(
      Scalar(
        Metadata(
          "id" -> "time",
          "type" -> "string",
          "class" -> "latis.time.Time",
          "units" -> "yyyy-MM-dd"
        )
      ),
      Scalar(
        Metadata(
          "id" -> "value",
          "type" -> "int"
        )
      )
    )

    val samples: MemoizedFunction = SampledFunction(granules(1))

    new MemoizedDataset(md, model, samples)
  }

  // Dataset for third granule
  private val g3: Dataset = {
    val md: Metadata = Metadata("g3")

    val model: DataType = Function(
      Scalar(
        Metadata(
          "id" -> "time",
          "type" -> "string",
          "class" -> "latis.time.Time",
          "units" -> "yyyy-MM-dd"
        )
      ),
      Scalar(
        Metadata(
          "id" -> "value",
          "type" -> "int"
        )
      )
    )

    val samples: MemoizedFunction = SampledFunction(granules(2))

    new MemoizedDataset(md, model, samples)
  }

  // Granule list dataset
  private val granuleList: Dataset = {
    val md: Metadata = Metadata("granuleList")

    val model: DataType = Function(
      Scalar(
        Metadata(
          "id" -> "time",
          "type" -> "string",
          "class" -> "latis.time.Time",
          "units" -> "yyyy-MM-dd"
        )
      ),
      Scalar(
        Metadata(
          "id" -> "uri",
          "type" -> "string"
        )
      )
    )

    val samples: MemoizedFunction = SampledFunction(
      List(
        Sample(List(StringValue("2020-01-01")), List(StringValue("file:///1"))),
        Sample(List(StringValue("2020-01-04")), List(StringValue("file:///2"))),
        Sample(List(StringValue("2020-01-07")), List(StringValue("file:///3")))
      )
    )

    new MemoizedDataset(md, model, samples)
  }

  // A very simple adapter.
  private def template(uri: URI): Dataset = uri.toString() match {
    case "file:///1" => g1
    case "file:///2" => g2
    case "file:///3" => g3
  }
}

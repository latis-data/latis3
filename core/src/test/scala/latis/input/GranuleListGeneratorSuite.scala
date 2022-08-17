package latis.input

import java.net.URI
import java.time.LocalDateTime

import cats.syntax.all._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.input.GranuleListGenerator.Config
import latis.metadata.Metadata
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.model.StringValueType
import latis.time.Time
import latis.util.Duration
import latis.util.Identifier._

class GranuleListGeneratorSuite extends munit.CatsEffectSuite {

  test("generate a list of granules") {
    val adapter = {
      val config = Config(
        LocalDateTime.of(2022, 1, 1, 0, 0, 0),
        LocalDateTime.of(2022, 1, 4, 0, 0, 0),
        Duration.fromIsoString("P1D").getOrElse(
          fail("Failed to construct Duration")
        ),
        "/%1$tY/%1$tm/%1$td/file-%1$tY-%1$tm-%1$td.ext"
      )

      val model: DataType = (
        Time.fromMetadata(
          Metadata(
            "id" -> "time",
            "type" -> "string",
            "units" -> "yyyy-MM-dd"
          )
        ),
        Scalar(id"uri", StringValueType).asRight
      ).flatMapN(Function.from).fold(
        err => fail("Failed to construct Function", clues(err)),
        identity
      )

      new GranuleListGenerator(model, config)
    }

    val expected = List(
      Sample(DomainData("2022-01-01T00:00:00.000Z"), RangeData("https://host/2022/01/01/file-2022-01-01.ext")),
      Sample(DomainData("2022-01-02T00:00:00.000Z"), RangeData("https://host/2022/01/02/file-2022-01-02.ext")),
      Sample(DomainData("2022-01-03T00:00:00.000Z"), RangeData("https://host/2022/01/03/file-2022-01-03.ext")),
      Sample(DomainData("2022-01-04T00:00:00.000Z"), RangeData("https://host/2022/01/04/file-2022-01-04.ext"))
    )

    adapter
      .getData(new URI("https://host"))
      .samples
      .compile
      .toList
      .assertEquals(expected)
  }

  test("config: build config from ConfigLike") {
    val conf = Config.fromConfigLike(
      AdapterConfig(
        "class" -> "",
        "start" -> "2022-01-02T03:04:05",
        "end" -> "2023-05-04T03:02:01",
        "step" -> "P1D",
        "pattern" -> "%1$tY-%1$tm-%1$td"
      )
    )

    val expected = Config(
      LocalDateTime.of(2022, 1, 2, 3, 4, 5),
      LocalDateTime.of(2023, 5, 4, 3, 2, 1),
      Duration.fromIsoString("P1D").getOrElse(
        fail("Failed to construct Duration")
      ),
      "%1$tY-%1$tm-%1$td"
    )

    assertEquals(conf, expected.asRight)
  }

  test("config: end time is optional") {
    val now = LocalDateTime.of(2022, 1, 3, 0, 0, 0)

    val conf = Config.fromConfigLike(
      now,
      AdapterConfig(
        "class" -> "",
        "start" -> "2022-01-02T03:04:05",
        "step" -> "P1D",
        "pattern" -> "%1$tY-%1$tm-%1$td"
      )
    )

    val expected = Config(
      LocalDateTime.of(2022, 1, 2, 3, 4, 5),
      now,
      Duration.fromIsoString("P1D").getOrElse(
        fail("Failed to construct Duration")
      ),
      "%1$tY-%1$tm-%1$td"
    )

    assertEquals(conf, expected.asRight)
  }

  test("config: start time is required") {
    val conf = Config.fromConfigLike(
      AdapterConfig(
        "class" -> "",
        "step" -> "P1D",
        "pattern" -> "%1$tY-%1$tm-%1$td"
      )
    )

    assert(conf.isLeft)
  }

  test("config: step is required") {
    val conf = Config.fromConfigLike(
      AdapterConfig(
        "class" -> "",
        "start" -> "2022-01-02T03:04:05",
        "pattern" -> "%1$tY-%1$tm-%1$td"
      )
    )

    assert(conf.isLeft)
  }

  test("config: pattern is required") {
    val conf = Config.fromConfigLike(
      AdapterConfig(
        "class" -> "",
        "start" -> "2022-01-02T03:04:05",
        "step" -> "P1D"
      )
    )

    assert(conf.isLeft)
  }

  test("config: require end > start") {
    val conf = Config.fromConfigLike(
      AdapterConfig(
        "class" -> "",
        "start" -> "2022-01-02",
        "end" -> "2022-01-01",
        "step" -> "P1D",
        "pattern" -> "%1$tY-%1$tm-%1$td"
      )
    )

    assert(conf.isLeft)
  }
}


package latis.input

import cats.Eq
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import fs2.io.file.*
import munit.CatsEffectSuite

import latis.data.*
import latis.dsl.ModelParser
import latis.input.FileListAdapter.Config
import latis.model.*

class FileListAdapterSuite extends CatsEffectSuite {

  private val flatDir = ResourceFunFixture {
    Files[IO].tempDirectory.evalTap { dir =>
      Files[IO].createFile(dir / "2010-a") >>
      Files[IO].createFile(dir / "2011-b") >>
      Files[IO].createFile(dir / "2012-c")
    }
  }

  private val nestedDir = ResourceFunFixture {
    Files[IO].tempDirectory.evalTap { dir =>
      Files[IO].createDirectory(dir / "a") >>
      Files[IO].createDirectory(dir / "b") >>
      Files[IO].createDirectory(dir / "c") >>
      Files[IO].createFile(dir / "a" / "2010-a") >>
      Files[IO].createFile(dir / "b" / "2011-b") >>
      Files[IO].createFile(dir / "c" / "2012-c")
    }
  }

  flatDir.test("list matching files in a flat directory") { root =>
    val adapter = {
      val config = Config(NonEmptyList.of(raw"(\d{4})-[ac]".r), None, None)
      val model: DataType = ModelParser.unsafeParse("time: string -> uri: string")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData(root.resolve("2010-a").toNioPath.toUri().toString())),
      Sample(DomainData("2012"), RangeData(root.resolve("2012-c").toNioPath.toUri().toString()))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.assertEquals(expected)
  }

  nestedDir.test("list matching files in subdirectories") { root =>
    val adapter = {
      val config = Config(NonEmptyList.of("[ac]".r, raw"(\d{4})-[ac]".r), None, None)
      val model: DataType = ModelParser.unsafeParse("time: string -> uri: string")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData(root.resolve("a/2010-a").toNioPath.toUri().toString())),
      Sample(DomainData("2012"), RangeData(root.resolve("c/2012-c").toNioPath.toUri().toString()))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.assertEquals(expected)
  }

  nestedDir.test("make URIs relative to baseDir config") { root =>
    val adapter = {
      val config = Config(NonEmptyList.of("[abc]".r, raw"(\d{4})-[abc]".r), None, Option(root))
      val model: DataType = ModelParser.unsafeParse("time: string -> uri: string")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData("a/2010-a")),
      Sample(DomainData("2011"), RangeData("b/2011-b")),
      Sample(DomainData("2012"), RangeData("c/2012-c"))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.assertEquals(expected)
  }

  flatDir.test("include file size when size scalar is present") { root =>
    val adapter = {
      val config = Config(NonEmptyList.of(raw"(\d{4})-[abc]".r), None, None)
      val model: DataType = ModelParser.unsafeParse("time: string -> (uri: string, size: long)")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData(root.resolve("2010-a").toNioPath.toUri().toString(), 0L)),
      Sample(DomainData("2011"), RangeData(root.resolve("2011-b").toNioPath.toUri().toString(), 0L)),
      Sample(DomainData("2012"), RangeData(root.resolve("2012-c").toNioPath.toUri().toString(), 0L))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.assertEquals(expected)
  }

  flatDir.test("construct domains with multiple scalars from matches") { root =>
    val adapter = {
      val config = Config(NonEmptyList.of(raw"(\d{4})-([abc])".r), None, None)
      val model: DataType = ModelParser.unsafeParse("(time: string, type: string) -> uri: string")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010", "a"), RangeData(root.resolve("2010-a").toNioPath.toUri().toString())),
      Sample(DomainData("2011", "b"), RangeData(root.resolve("2011-b").toNioPath.toUri().toString())),
      Sample(DomainData("2012", "c"), RangeData(root.resolve("2012-c").toNioPath.toUri().toString()))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.assertEquals(expected)
  }

  test("capture matches from any part of the pattern") {
    val adapter = {
      val config = Config(NonEmptyList.of(raw"(\d{4})".r, "a".r), None, None)
      val model: DataType = ModelParser.unsafeParse("time: string -> uri: string")
      new FileListAdapter(model, config)
    }

    val dir = Files[IO].tempDirectory.evalTap { dir =>
      Files[IO].createDirectory(dir / Path("2010")) >>
      Files[IO].createDirectory(dir / Path("2011")) >>
      Files[IO].createDirectory(dir / Path("2012")) >>
      Files[IO].createFile(dir / Path("2010/a"))
    }

    dir.use { root =>
      val expected: List[Sample] = List(
        Sample(DomainData("2010"), RangeData(root.resolve("2010/a").toNioPath.toUri().toString()))
      )

      adapter.getData(root.toNioPath.toUri()).samples.compile.toList.assertEquals(expected)
    }
  }

  flatDir.test("arrange matches based on the column specification") { root =>
    val adapter = {
      val config = Config(
        NonEmptyList.of(raw"(\d{4})-([abc])".r),
        Option(List(List(1), List(0))),
        None
      )
      val model: DataType = ModelParser.unsafeParse("(type: string, time: string) -> uri: string")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("a", "2010"), RangeData(root.resolve("2010-a").toNioPath.toUri().toString())),
      Sample(DomainData("b", "2011"), RangeData(root.resolve("2011-b").toNioPath.toUri().toString())),
      Sample(DomainData("c", "2012"), RangeData(root.resolve("2012-c").toNioPath.toUri().toString()))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.assertEquals(expected)
  }

  test("join columns separated by commas in the specifiation") {
    // Create the adapter.
    val adapter = {
      val config = Config(
        NonEmptyList.of(raw"(\d{2})(\d{4})(\d{2})-[abc]".r),
        Option(List(List(1, 2, 0))),
        None
      )
      val model: DataType = ModelParser.unsafeParse("time: string -> uri: string")
      new FileListAdapter(model, config)
    }

    Files[IO].tempDirectory.use { dir =>
      Files[IO].createFile(dir / "01201001-a") >>
      Files[IO].createFile(dir / "02201106-b") >>
      Files[IO].createFile(dir / "03201212-c") >> {
        val expected: List[Sample] = List(
          Sample(DomainData("2010 01 01"), RangeData(dir.resolve("01201001-a").toNioPath.toUri().toString())),
          Sample(DomainData("2011 06 02"), RangeData(dir.resolve("02201106-b").toNioPath.toUri().toString())),
          Sample(DomainData("2012 12 03"), RangeData(dir.resolve("03201212-c").toNioPath.toUri().toString()))
        )
        adapter.getData(dir.toNioPath.toUri()).samples.compile.toList.assertEquals(expected)
      }
    }
  }

  // We need to override how Configs are compared for equality because
  // we can't compare regular expressions for equality.
  private implicit val configEq: Eq[Config] = new Eq[Config] {
    override def eqv(a: Config, b: Config): Boolean =
      a.pattern.toString() == b.pattern.toString() &&
      a.columns == b.columns &&
      a.baseDir == b.baseDir
  }

  test("config is buildable from a ConfigLike") {
    val conf = Config.fromConfigLike(
      AdapterConfig(
        "class" -> "",
        "pattern" -> raw"(\d{4})",
        "columns" -> "1,2,3;4;5",
        "baseDir" -> "file:///base/dir"
      )
    ).fold(throw _, identity)

    val expected = Config(
      NonEmptyList.of(raw"(\d{4})".r),
      Option(List(List(1, 2, 3), List(4), List(5))),
      Option(Path("/base/dir"))
    )

    // need to use custom equality
    assert(conf === expected)
  }

  test("config splits pattern by '/'") {
    val conf = Config.fromConfigLike(
      AdapterConfig(
        "class" -> "",
        "pattern" -> raw"[abc]/(\d{4})",
        "columns" -> "1,2,3;4;5",
        "baseDir" -> "file:///base/dir"
      )
    ).fold(throw _, identity)

    val expected = Config(
      NonEmptyList.of("[abc]".r, raw"(\d{4})".r),
      Option(List(List(1, 2, 3), List(4), List(5))),
      Option(Path("/base/dir"))
    )

    // need to use custom equality
    assert(conf === expected)
  }

  test("config requires a pattern definition") {
    val conf = Config.fromConfigLike(AdapterConfig("class" -> ""))

    assert(conf.isLeft)
  }

  test("config requires numeric columns") {
    val conf = Config.fromConfigLike(
      AdapterConfig("class" -> "", "columns" -> "1; a; 3")
    )

    assert(conf.isLeft)
  }

  test("config requires file URI for baseDir") {
    val conf = Config.fromConfigLike(
      AdapterConfig("class" -> "", "baseDir" -> "base dir")
    )

    assert(conf.isLeft)
  }
}

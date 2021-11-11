package latis.input

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.file._
import org.scalactic.Equality
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.dsl.ModelParser
import latis.input.FileListAdapter.Config
import latis.model._
import latis.util.LatisException

class FileListAdapterSpec extends AnyFlatSpec {

  "A file list adapter" should "list files in a flat directory" in withFlatDir { root =>
    val adapter = {
      val config = Config(raw"(\d{4})-[abc]".r, None, None)
      val model: DataType = ModelParser.unsafeParse("time: string -> uri: string")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData(root.resolve("2010-a").toNioPath.toUri().toString())),
      Sample(DomainData("2011"), RangeData(root.resolve("2011-b").toNioPath.toUri().toString())),
      Sample(DomainData("2012"), RangeData(root.resolve("2012-c").toNioPath.toUri().toString()))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.map { samples =>
      samples should contain theSameElementsAs (expected)
    }
  }.unsafeRunSync()

  it should "list files in subdirectories" in withNestedDir { root =>
    val adapter = {
      val config = Config(raw"(\d{4})-[abc]".r, None, None)
      val model: DataType = ModelParser.unsafeParse("time: string -> uri: string")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData(root.resolve("a/2010-a").toNioPath.toUri().toString())),
      Sample(DomainData("2011"), RangeData(root.resolve("b/2011-b").toNioPath.toUri().toString())),
      Sample(DomainData("2012"), RangeData(root.resolve("c/2012-c").toNioPath.toUri().toString()))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.map { samples =>
      samples should contain theSameElementsAs (expected)
    }
  }.unsafeRunSync()

  it should "make URIs relative to baseDir config" in withNestedDir { root =>
    val adapter = {
      val config = Config(raw"(\d{4})-[abc]".r, None, Option(root))
      val model: DataType = ModelParser.unsafeParse("time: string -> uri: string")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData("a/2010-a")),
      Sample(DomainData("2011"), RangeData("b/2011-b")),
      Sample(DomainData("2012"), RangeData("c/2012-c"))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.map { samples =>
      samples should contain theSameElementsAs (expected)
    }
  }.unsafeRunSync()

  it should "include file size when size scalar is present" in withFlatDir { root =>
    val adapter = {
      val config = Config(raw"(\d{4})-[abc]".r, None, None)
      val model: DataType = ModelParser.unsafeParse("time: string -> (uri: string, size: long)")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData(root.resolve("2010-a").toNioPath.toUri().toString(), 0L)),
      Sample(DomainData("2011"), RangeData(root.resolve("2011-b").toNioPath.toUri().toString(), 0L)),
      Sample(DomainData("2012"), RangeData(root.resolve("2012-c").toNioPath.toUri().toString(), 0L))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.map { samples =>
      samples should contain theSameElementsAs (expected)
    }
  }.unsafeRunSync()

  it should "silently drop files that don't match the pattern" in withFlatDir { root =>
    val adapter = {
      // Note this pattern will exclude 'b'
      val config = Config(raw"(\d{4})-[ac]".r, None, None)
      val model: DataType = ModelParser.unsafeParse("time: string -> uri: string")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData(root.resolve("2010-a").toNioPath.toUri().toString())),
      Sample(DomainData("2012"), RangeData(root.resolve("2012-c").toNioPath.toUri().toString()))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.map { samples =>
      samples should contain theSameElementsAs (expected)
    }
  }.unsafeRunSync()

  it should "construct domains with multiple scalars from matches" in withFlatDir { root =>
    val adapter = {
      val config = Config(raw"(\d{4})-([abc])".r, None, None)
      val model: DataType = ModelParser.unsafeParse("(time: string, type: string) -> uri: string")
      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010", "a"), RangeData(root.resolve("2010-a").toNioPath.toUri().toString())),
      Sample(DomainData("2011", "b"), RangeData(root.resolve("2011-b").toNioPath.toUri().toString())),
      Sample(DomainData("2012", "c"), RangeData(root.resolve("2012-c").toNioPath.toUri().toString()))
    )

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.map { samples =>
      samples should contain theSameElementsAs (expected)
    }
  }.unsafeRunSync()

  it should "arrange matches based on the column specification" in withFlatDir { root =>
    val adapter = {
      val config = Config(
        raw"(\d{4})-([abc])".r,
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

    adapter.getData(root.toNioPath.toUri()).samples.compile.toList.map { samples =>
      samples should contain theSameElementsAs (expected)
    }
  }.unsafeRunSync()

  it should "join columns separated by commas in the specifiation" in {
    // Create the adapter.
    val adapter = {
      val config = Config(
        raw"(\d{2})(\d{4})(\d{2})-[abc]".r,
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
        adapter.getData(dir.toNioPath.toUri()).samples.compile.toList.map { samples =>
          samples should contain theSameElementsAs (expected)
        }
      }
    }.unsafeRunSync()
  }

  // We need to override how Configs are compared for equality because
  // we can't compare regular expressions for equality.
  private implicit val configEquality: Equality[Config] = new Equality[Config] {
    override def areEqual(a: Config, b: Any): Boolean = b match {
      case b: Config =>
        a.pattern.toString() == b.pattern.toString() &&
        a.columns == b.columns &&
        a.baseDir == b.baseDir
      case _         => false
    }
  }

  "A file list adapter config" should "be buildable from a ConfigLike" in {
    val conf = Config.fromConfigLike(
      AdapterConfig(
        "class" -> "",
        "pattern" -> raw"(\d{4})",
        "columns" -> "1,2,3;4;5",
        "baseDir" -> "file:///base/dir"
      )
    ).fold(throw _, identity)

    val expected = Config(
      raw"(\d{4})".r,
      Option(List(List(1, 2, 3), List(4), List(5))),
      Option(Path("/base/dir"))
    )

    conf should equal (expected)
  }

  it should "require a pattern definition" in {
    val conf = Config.fromConfigLike(AdapterConfig("class" -> ""))

    conf.left.value shouldBe a [LatisException]
  }

  it should "require numeric columns" in {
    val conf = Config.fromConfigLike(
      AdapterConfig("class" -> "", "columns" -> "1; a; 3")
    )

    conf.left.value shouldBe a [LatisException]
  }

  it should "require file URI for baseDir" in {
    val conf = Config.fromConfigLike(
      AdapterConfig("class" -> "", "baseDir" -> "base dir")
    )

    conf.left.value shouldBe a [LatisException]
  }


  private def withFlatDir(f: Path => IO[Any]): IO[Any] =
    Files[IO].tempDirectory.use { dir =>
      Files[IO].createFile(dir / "2010-a") >>
      Files[IO].createFile(dir / "2011-b") >>
      Files[IO].createFile(dir / "2012-c") >>
      f(dir)
    }

  private def withNestedDir(f: Path => IO[Any]): IO[Any] =
    Files[IO].tempDirectory.use { dir =>
      Files[IO].createDirectory(dir / "a") >>
      Files[IO].createDirectory(dir / "b") >>
      Files[IO].createDirectory(dir / "c") >>
      Files[IO].createFile(dir / "a" / "2010-a") >>
      Files[IO].createFile(dir / "b" / "2011-b") >>
      Files[IO].createFile(dir / "c" / "2012-c") >>
      f(dir)
    }
}

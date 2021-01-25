package latis.input

import java.io.IOException
import java.nio.file.Files
import java.nio.file.FileVisitResult
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes

import org.scalactic.Equality
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.util.LatisException
import latis.util.StreamUtils
import FileListAdapter.Config

class FileListAdapterSpec extends AnyFlatSpec {

  "A file list adapter" should "list files in a flat directory" in withFlatDir { root =>
    val adapter = {
      val config = Config(raw"(\d{4})-[abc]".r, None, None)

      val model: DataType = Function(
        Scalar(Metadata(
          "id" -> "time",
          "class" -> "latis.time.Time",
          "units" -> "yyyy",
          "type" -> "string"
        )),
        Scalar(Metadata(
          "id" -> "uri",
          "type" -> "string"
        ))
      )

      new FileListAdapter(model, config)
    }

    val samples: List[Sample] =
      adapter.getData(root.toUri()).samples.compile.toList.unsafeRunSync

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData(root.resolve("2010-a").toUri().toString())),
      Sample(DomainData("2011"), RangeData(root.resolve("2011-b").toUri().toString())),
      Sample(DomainData("2012"), RangeData(root.resolve("2012-c").toUri().toString()))
    )

    samples should contain theSameElementsAs (expected)
  }

  it should "list files in subdirectories" in withNestedDir { root =>
    val adapter = {
      val config = Config(raw"(\d{4})-[abc]".r, None, None)

      val model: DataType = Function(
        Scalar(Metadata(
          "id" -> "time",
          "class" -> "latis.time.Time",
          "units" -> "yyyy",
          "type" -> "string"
        )),
        Scalar(Metadata(
          "id" -> "uri",
          "type" -> "string"
        ))
      )

      new FileListAdapter(model, config)
    }

    val samples: List[Sample] =
      adapter.getData(root.toUri()).samples.compile.toList.unsafeRunSync

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData(root.resolve("a/2010-a").toUri().toString())),
      Sample(DomainData("2011"), RangeData(root.resolve("b/2011-b").toUri().toString())),
      Sample(DomainData("2012"), RangeData(root.resolve("c/2012-c").toUri().toString()))
    )

    samples should contain theSameElementsAs (expected)
  }

  it should "make URIs relative to baseDir config" in withNestedDir { root =>
    val adapter = {
      val config = Config(raw"(\d{4})-[abc]".r, None, Option(root))

      val model: DataType = Function(
        Scalar(Metadata(
          "id" -> "time",
          "class" -> "latis.time.Time",
          "units" -> "yyyy",
          "type" -> "string"
        )),
          Scalar(Metadata(
            "id" -> "uri",
            "type" -> "string"
          ))
      )

      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData("a/2010-a")),
      Sample(DomainData("2011"), RangeData("b/2011-b")),
      Sample(DomainData("2012"), RangeData("c/2012-c"))
    )

    val samples: List[Sample] =
      adapter.getData(root.toUri()).samples.compile.toList.unsafeRunSync

    samples should contain theSameElementsAs (expected)
  }

  it should "include file size when size scalar is present" in withFlatDir { root =>
    val adapter = {
      val config = Config(raw"(\d{4})-[abc]".r, None, None)

      val model: DataType = Function(
        Scalar(Metadata(
          "id" -> "time",
          "class" -> "latis.time.Time",
          "units" -> "yyyy",
          "type" -> "string"
        )),
        Tuple(
          Scalar(Metadata(
            "id" -> "uri",
            "type" -> "string"
          )),
          Scalar(Metadata(
            "id" -> "size",
            "type" -> "long"
          ))
        )
      )

      new FileListAdapter(model, config)
    }

    val samples: List[Sample] =
      adapter.getData(root.toUri()).samples.compile.toList.unsafeRunSync

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData(root.resolve("2010-a").toUri().toString(), 0l)),
      Sample(DomainData("2011"), RangeData(root.resolve("2011-b").toUri().toString(), 0l)),
      Sample(DomainData("2012"), RangeData(root.resolve("2012-c").toUri().toString(), 0l))
    )

    samples should contain theSameElementsAs (expected)
  }

  it should "silently drop files that don't match the pattern" in withFlatDir { root =>
    val adapter = {
      // Note this pattern will exclude 'b'
      val config = Config(raw"(\d{4})-[ac]".r, None, None)

      val model: DataType = Function(
        Scalar(Metadata(
          "id" -> "time",
          "class" -> "latis.time.Time",
          "units" -> "yyyy",
          "type" -> "string"
        )),
        Scalar(Metadata(
          "id" -> "uri",
          "type" -> "string"
        ))
      )

      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010"), RangeData(root.resolve("2010-a").toUri().toString())),
      Sample(DomainData("2012"), RangeData(root.resolve("2012-c").toUri().toString()))
    )

    val samples: List[Sample] =
      adapter.getData(root.toUri()).samples.compile.toList.unsafeRunSync

    samples should contain theSameElementsAs (expected)
  }

  it should "construct domains with multiple scalars from matches" in withFlatDir { root =>
    val adapter = {
      val config = Config(raw"(\d{4})-([abc])".r, None, None)

      val model: DataType = Function(
        Tuple(
          Scalar(Metadata(
            "id" -> "time",
            "class" -> "latis.time.Time",
            "units" -> "yyyyMMdd",
            "type" -> "string"
          )),
          Scalar(Metadata(
            "id" -> "type",
            "type" -> "string"
          ))
        ),
        Scalar(Metadata(
          "id" -> "uri",
          "type" -> "string"
        ))
      )

      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010", "a"), RangeData(root.resolve("2010-a").toUri().toString())),
      Sample(DomainData("2011", "b"), RangeData(root.resolve("2011-b").toUri().toString())),
      Sample(DomainData("2012", "c"), RangeData(root.resolve("2012-c").toUri().toString()))
    )

    val samples: List[Sample] =
      adapter.getData(root.toUri()).samples.compile.toList.unsafeRunSync

    samples should contain theSameElementsAs (expected)
  }

  it should "arrange matches based on the column specification" in withFlatDir { root =>
    val adapter = {
      val config = Config(
        raw"(\d{4})-([abc])".r,
        Option(List(List(1), List(0))),
        None
      )

      val model: DataType = Function(
        Tuple(
          Scalar(Metadata(
            "id" -> "type",
            "type" -> "string"
          )),
          Scalar(Metadata(
            "id" -> "time",
            "class" -> "latis.time.Time",
            "units" -> "yyyy",
            "type" -> "string"
          ))
        ),
        Scalar(Metadata(
          "id" -> "uri",
          "type" -> "string"
        ))
      )

      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("a", "2010"), RangeData(root.resolve("2010-a").toUri().toString())),
      Sample(DomainData("b", "2011"), RangeData(root.resolve("2011-b").toUri().toString())),
      Sample(DomainData("c", "2012"), RangeData(root.resolve("2012-c").toUri().toString()))
    )

    val samples: List[Sample] =
      adapter.getData(root.toUri()).samples.compile.toList.unsafeRunSync

    samples should contain theSameElementsAs (expected)
  }

  it should "join columns separated by commas in the specifiation" in {
    // Create the directory.
    val root: Path = {
      val dir = Files.createTempDirectory(null)
      Files.createFile(dir.resolve("01201001-a"))
      Files.createFile(dir.resolve("02201106-b"))
      Files.createFile(dir.resolve("03201212-c"))

      dir
    }

    // Create the adapter.
    val adapter = {
      val config = Config(
        raw"(\d{2})(\d{4})(\d{2})-[abc]".r,
        Option(List(List(1, 2, 0))),
        None
      )

      val model: DataType = Function(
        Scalar(Metadata(
          "id" -> "time",
          "class" -> "latis.time.Time",
          "units" -> "yyyy MM dd",
          "type" -> "string"
        )),
        Scalar(Metadata(
          "id" -> "uri",
          "type" -> "string"
        ))
      )

      new FileListAdapter(model, config)
    }

    val expected: List[Sample] = List(
      Sample(DomainData("2010 01 01"), RangeData(root.resolve("01201001-a").toUri().toString())),
      Sample(DomainData("2011 06 02"), RangeData(root.resolve("02201106-b").toUri().toString())),
      Sample(DomainData("2012 12 03"), RangeData(root.resolve("03201212-c").toUri().toString()))
    )

    // Run the test.
    withTmpDir(root) { dir =>
      val samples: List[Sample] =
        adapter.getData(dir.toUri()).samples.compile.toList.unsafeRunSync

      samples should contain theSameElementsAs (expected)
    }
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
      Option(Paths.get("/base/dir"))
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

  private def withTmpDir(tmp: Path)(f: Path => Any): Any =
    try f(tmp) finally deleteTmpDir(tmp)

  private def deleteTmpDir(tmp: Path): Unit = {
    val visitor = new SimpleFileVisitor[Path] {
      override def visitFile(p: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.deleteIfExists(p)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(p: Path, e: IOException): FileVisitResult = {
        Files.deleteIfExists(p)
        FileVisitResult.CONTINUE
      }
    }

    Files.walkFileTree(tmp, visitor)
  }

  private def withFlatDir(f: Path => Any): Any = {
    val dir = Files.createTempDirectory(null)
    Files.createFile(dir.resolve("2010-a"))
    Files.createFile(dir.resolve("2011-b"))
    Files.createFile(dir.resolve("2012-c"))

    withTmpDir(dir)(f)
  }

  private def withNestedDir(f: Path => Any): Any = {
    val dir = Files.createTempDirectory(null)
    val dirA = Files.createDirectory(dir.resolve("a"))
    val dirB = Files.createDirectory(dir.resolve("b"))
    val dirC = Files.createDirectory(dir.resolve("c"))
    Files.createFile(dirA.resolve("2010-a"))
    Files.createFile(dirB.resolve("2011-b"))
    Files.createFile(dirC.resolve("2012-c"))

    withTmpDir(dir)(f)
  }
}

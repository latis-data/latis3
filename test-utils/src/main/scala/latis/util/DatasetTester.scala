package latis.util

import scala.concurrent.duration.DurationInt
import scala.util.matching.Regex

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.all.*
import fs2.text
import fs2.Stream
import fs2.io.file.*

import latis.catalog.Catalog
import latis.catalog.FdmlCatalog
import latis.data.Data
import latis.data.Data.DoubleValue
import latis.data.Data.FloatValue
import latis.data.Sample
import latis.dsl.DatasetOps
import latis.input.TextAdapter
import latis.util.StringUtils.*

class DatasetTester(catalog: Catalog) {

  /**
   *  Defines test data pattern.
   *
   *  The expected form is "datasetId, val1, val2, ...".
   */
  private val pattern: Regex = """(^[a-zA-Z_]\w*),(.*)""".r
  //TODO: don't silently drop invalid lines?

  /** Tests each line in the test data file. */
  def testFile(path: Path): Stream[IO, Boolean] =
    Files[IO]
      .readAll(path)
      .through(text.utf8.decode)
      .through(text.lines)
      .filter(pattern.matches)
      .evalMap(testLine)

  /**
   * Determines if the given samples match.
   *
   * Nested Functions are not supported.
   */
  private def matchSamples(s1: Sample, s2: Sample): Boolean =
    s1.domain.length == s2.domain.length &&
      s1.range.length == s2.range.length &&
      (s1.domain ++ s1.range).zip(s2.domain ++ s2.range).forall(matchData)

  /**
   * Determines if the given data match.
   *
   * NaNs are treated as a match, unlike equality.
   */
  private def matchData(pair: (Data, Data)): Boolean = pair match {
    case (a: DoubleValue, b: DoubleValue) => (a.value.isNaN && b.value.isNaN) || a == b
    case (a: FloatValue, b: FloatValue)   => (a.value.isNaN && b.value.isNaN) || a == b
    case (a, b)                           => a == b
  }

  /** Tests if the first sample of the given dataset matches the given data. */
  private def testData(dataset: String, data: String): IO[Boolean] = for {
    _    <- IO.print(s"$dataset: ")
    id   <- IO.fromOption(Identifier.fromString(dataset))(LatisException(" Invalid identifier"))
    ods  <- catalog.findDataset(id)
    ds   <- IO.fromOption(ods)(LatisException(" Dataset not found"))
    adapter <- IO(TextAdapter(ds.model, new TextAdapter.Config("delimiter" -> """\s*,\s*""")))
    testSample <- IO.fromOption(adapter.parseRecord(data)) {
      LatisException(s" Failed\n  Unable to parse test data: $data\n                 with model: ${ds.model}")
    }
    oSample <- ds.head().samples.compile.toList.map(_.headOption)
    firstSample <- IO.fromOption(oSample) {
      LatisException(" Empty dataset")
    }
    test <- testSamples(firstSample, testSample)
  } yield test

  private def testSamples(s1: Sample, s2: Sample): IO[Boolean] = {
    if (matchSamples(s1, s2)) IO.println(s" Passed".green) *> true.pure[IO]
    else
      IO.println(s" Failed".red) *>
      IO.println(s"  Expected: $s2".red) *>
      IO.println(s"  Actual:   $s1".red) *>
      false.pure[IO]
  }

  /** Prints a dot (".") every second. */
  private lazy val dots: IO[Unit] = IO.sleep(1000.milliseconds).flatMap(_ => IO.print(".")).foreverM
  //TODO: add timeout duration so error if this wins the race!
  //  or just put timeout on testData, raises error

  /** Processes a line of test data, */
  def testLine(line: String): IO[Boolean] = {
    (line match {
      case pattern(name, data) =>
        //Print dots while processing data
        dots.race(testData(name, data)) //IO[Either[Unit, Boolean]]
      case _ => ??? // Not possible due to filter in testFile
    }).attempt.flatMap {
      case Left(t) => IO.println(t.getMessage.red) *> false.pure[IO]
      case Right(Right(b)) => b.pure[IO]
      case _ => ??? //forever dots won the race
    }
  }

}


object DatasetTester extends IOApp {
  //TODO: enable via service call
  //TODO: support "+-" tolerance
  //TODO: use log level for more detail?
  //TODO: validate fdml
  //TODO: add timeout and memory monitor for each dataset test
  //TODO: consider using ScalaTest

  private lazy val catalog: IO[Catalog] = {
    val dir = LatisConfig.getOrElse("latis.fdml.dir", "datasets/fdml")
    FdmlCatalog.fromDirectory(Path(dir), false)
  }

  def run(args: List[String]): IO[ExitCode] = {
    val file = args.headOption.getOrElse("datasets/testData.csv")

    // Make a stream of tests with console output baked in
    val tests = for {
      cat <- Stream.eval(catalog)
      path = Path(file)
      tests <- new DatasetTester(cat).testFile(path)
    } yield tests

    // Create a report
    tests.fold((0, 0)) { //count (pass, fail)
      case ((p, f), b) =>
        if (b) (p + 1, f)
        else (p, f + 1)
    }.compile.toList.flatMap {
      case (p, f) :: Nil =>
        val s = if (p == 1) "" else "s"
        //IO.println(s"$p test$s passed.".green) //not printing
        println(s"$p test$s passed.".green)
        if (f > 0) {
          val s = if (f > 1) "s" else ""
          IO.println(s"$f test$s failed.".red) *>
            ExitCode.Error.pure[IO]
        } else ExitCode.Success.pure[IO]
      case _ => ExitCode.Error.pure[IO] //fold guarantees a single element
    }
  }
}

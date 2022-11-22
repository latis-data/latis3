package latis.input

import scala.xml.XML

import cats.effect._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2._
import fs2.io.file._
import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite

import latis.input.fdml.Fdml
import latis.input.fdml.FdmlParser
import latis.input.fdml.FdmlReader
import latis.ops.OperationRegistry
import latis.ops.Selection
import latis.util.LatisException

class GranuleAppendDatasetSuite extends AnyFunSuite {

  // Make three csv files with threes samples each.
  private def makeGranules(dir: Path): IO[Unit] = {
    Stream(1,2,3).flatMap { n =>
      Stream(
        s"$n.0,a$n\n",
        s"$n.4,b$n\n",
        s"$n.8,c$n\n"
      )
        .through(text.utf8.encode)
        .through(Files[IO].writeAll(dir / Path(s"file$n.csv")))
    }.compile.drain
  }

  // Loan a set of granules in a tmp directory.
  private def withGranules(f: Path => IO[Any]): IO[Any] = {
    val resource = for {
      dir <- Files[IO].tempDirectory
      _   <- Resource.eval(makeGranules(dir))
    } yield dir
    resource.use(f)
  }

  // Make fdml for a granule append dataset.
  private def makeFdml(dir: Path): Either[LatisException, Fdml] = FdmlParser.parseXml(
    XML.loadString(s"""
      <dataset id="dataset" class="latis.dataset.GranuleAppendDataset">

        <source>
          <dataset id="granules">
            <source uri="file://$dir"/>
            <adapter class="latis.input.FileListAdapter"
                     pattern=".*file(\\d).csv"/>
            <function>
              <scalar id="x" type="float" binWidth="1"/>
              <scalar id="uri" type="string"/>
            </function>
          </dataset>
        </source>

        <adapter class="latis.input.TextAdapter"/>

        <function>
          <scalar id="x" type="float"/>
          <scalar id="a" type="string"/>
        </function>

      </dataset>
    """))


  test("length of granule append dataset from fdml") {
    withGranules { dir =>
      (for {
        fdml <- makeFdml(dir)
        ds   <- FdmlReader.read(fdml, OperationRegistry.default)
      } yield {
        ds.samples.compile.toList.map { samples =>
          assert(9 == samples.length)
        }
      }).value
    }.unsafeRunSync()
  }

  test("selection with bin semantics") {
    withGranules { dir =>
      (for {
        fdml <- makeFdml(dir)
        ds   <- FdmlReader.read(fdml, OperationRegistry.default)
      } yield {
        val ops = List(
          Selection.makeSelection("x > 1.5").value,
          Selection.makeSelection("x < 2.5").value,
        )
        ds.withOperations(ops).samples.compile.toList.map { samples =>
          assert(3 == samples.length) //1.8,2.0,2.4
        }
      }).value
    }.unsafeRunSync()
  }
}

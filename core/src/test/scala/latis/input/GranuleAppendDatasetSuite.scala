package latis.input

import scala.xml.XML

import cats.effect.IO
import cats.syntax.all._
import fs2._
import fs2.io.file._
import munit.CatsEffectSuite

import latis.input.fdml.Fdml
import latis.input.fdml.FdmlParser
import latis.input.fdml.FdmlReader
import latis.ops.OperationRegistry
import latis.ops.Selection
import latis.util.LatisException

class GranuleAppendDatasetSuite extends CatsEffectSuite {

  private val granules = ResourceFixture(
    Files[IO].tempDirectory.evalTap(makeGranules(_))
  )

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

  granules.test("length of granule append dataset from fdml") { dir =>
    IO.fromEither(makeFdml(dir)
      .flatMap(FdmlReader.read(_, OperationRegistry.default)))
      .flatMap(_.samples.compile.toList)
      .map(ss => assertEquals(ss.length, 9))
  }

  granules.test("selection with bin semantics") { dir =>
    (
      IO.fromEither(makeFdml(dir).flatMap(
        FdmlReader.read(_, OperationRegistry.default))
      ),
      IO.fromEither {
        List(
          Selection.makeSelection("x > 1.5"),
          Selection.makeSelection("x < 2.5")
        ).sequence
      }
    ).flatMapN(_.withOperations(_).samples.compile.toList)
      .map(ss => assertEquals(ss.length, 3))
  }
}

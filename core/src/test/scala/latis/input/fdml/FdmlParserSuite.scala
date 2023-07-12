package latis.input.fdml

import java.net.URI

import cats.syntax.all._
import munit.FunSuite

import latis.model.DoubleValueType
import latis.model.IntValueType
import latis.model.StringValueType
import latis.util.Identifier._
import latis.util.LatisException
import latis.util.NetUtils
import latis.util.dap2.parser.ast

// FDML files live in core/src/test/resources/fdml-parser.

final class FdmlParserSuite extends FunSuite {

  test("parse a valid FDML file") {
    withFdmlFile("fdml-parser/valid.fdml") {
      case Left(err) => fail(err.message)
      case Right(_: GranuleAppendFdml) => fail("got GranuleAppendFdml")
      case Right(DatasetFdml(metadata, source, adapter, model, operations)) =>
        assertEquals(
          metadata.properties,
          Map("id" -> "valid", "title" -> "Valid Dataset")
        )

        assertEquals(source, UriSource(new URI("file:///fake/path")))

        assertEquals(
          adapter,
          SingleAdapter(
            "latis.input.TextAdapter",
            Map("skipLines" -> "1", "class" -> "latis.input.TextAdapter")
          )
        )

        assertEquals(
          model,
          FFunction(
            FScalar(
              id"time",
              IntValueType,
              Map(
                "id" -> "time",
                "units" -> "days since 2000-01-01",
                "type" -> "int",
                "class" -> "latis.time.Time"
              )
            ),
            FFunction(
              FTuple(
                FScalar(
                  id"a",
                  IntValueType,
                  Map("id" -> "a", "type" -> "int")
                ),
                FScalar(
                  id"b",
                  DoubleValueType,
                  Map("id" -> "b", "type" -> "double")
                ),
                List(
                  FScalar(
                    id"c",
                    StringValueType,
                    Map("id" -> "c", "type" -> "string")
                  )
                ),
                Map("id" -> "inner_domain")
              ),
              FTuple(
                FScalar(
                  id"d",
                  IntValueType,
                  Map("id" -> "d", "type" -> "int")
                ),
                FScalar(
                  id"e",
                  DoubleValueType,
                  Map("id" -> "e", "type" -> "double")
                ),
                List(
                  FScalar(
                    id"f",
                    StringValueType,
                    Map("id" -> "f", "type" -> "string")
                  )
                ),
                Map("id" -> "inner_range")
              ),
              Map("id" -> "inner")
            ),
            Map("id" -> "outer")
          )
        )

        assertEquals(
          operations,
          List(
            ast.Selection(id"time", ast.Gt, "2000-01-01"),
            ast.Projection(List(id"a", id"b", id"c")),
            ast.Operation("rename", List("Constantinople", "Istanbul"))
          )
        )
    }
  }

  test("gracefully handle non-FDML files") {
    withFdmlFile("fdml-parser/not-fdml.fdml") {
      assertEquals(_, Left(LatisException("Expecting dataset element")))
    }
  }

  test("require a source element") {
    withFdmlFile("fdml-parser/no-source.fdml") {
      assertEquals(_, Left(LatisException("Expecting source element")))
    }
  }

  test("require a single source element") {
    withFdmlFile("fdml-parser/multiple-sources.fdml") {
      assertEquals(_, Left(LatisException("Expecting a single source")))
    }
  }

  test("require that sources have a URI attribute") {
    withFdmlFile("fdml-parser/no-source-uri.fdml") {
      assertEquals(_, Left(LatisException("Expecting source with uri attribute")))
    }
  }

  test("require a valid source URI") {
    withFdmlFile("fdml-parser/invalid-source-uri.fdml") { fdml =>
      assertEquals(
        // don't care about testing the cause
        fdml.leftMap(_.copy(cause = null)),
        Left(LatisException("Source URI is malformed"))
      )
    }
  }

  test("require an adapter element") {
    withFdmlFile("fdml-parser/no-adapter.fdml") {
      assertEquals(_, Left(LatisException("Expecting adapter element")))
    }
  }

  test("require a single adapter element") {
    withFdmlFile("fdml-parser/multiple-adapters.fdml") {
      assertEquals(_, Left(LatisException("Expecting a single adapter")))
    }
  }

  test("require that adapters have a class attribute") {
    withFdmlFile("fdml-parser/no-adapter-class.fdml") {
      assertEquals(_, Left(LatisException("Expecting adapter with class attribute")))
    }
  }

  test("require a model") {
    withFdmlFile("fdml-parser/no-model.fdml") {
      assertEquals(_, Left(LatisException("Expecting model starting with function")))
    }
  }

  test("require that models start with functions") {
    withFdmlFile("fdml-parser/model-not-starting-with-function.fdml") {
      assertEquals(_, Left(LatisException("Expecting model starting with function")))
    }
  }

  test("require that functions have a domain and range") {
    withFdmlFile("fdml-parser/function-without-range.fdml") {
      assertEquals(_, Left(LatisException("Expecting domain and range")))
    }
  }

  test("require a single root function") {
    withFdmlFile("fdml-parser/multiple-root-functions.fdml") {
      assertEquals(_, Left(LatisException("Expecting a single root function")))
    }
  }

  test("require that tuples have at least two children") {
    withFdmlFile("fdml-parser/one-element-tuple.fdml") {
      assertEquals(_, Left(LatisException("Expecting at least two children")))
    }
  }

  test("require that scalars have identifiers") {
    withFdmlFile("fdml-parser/scalar-without-identifier.fdml") {
      assertEquals(_, Left(LatisException("Expecting scalar with id attribute")))
    }
  }

  test("require that scalars have types") {
    withFdmlFile("fdml-parser/scalar-without-type.fdml") {
      assertEquals(_, Left(LatisException("Expecting scalar with type attribute")))
    }
  }

  test("require that scalars have valid types") {
    withFdmlFile("fdml-parser/scalar-without-valid-type.fdml") {
      case Left(e: LatisException) =>
        assert(e.message.startsWith("Invalid Scalar value type"))
      case _ => fail("")
    }
  }

  test("require that operations have expressions") {
    withFdmlFile("fdml-parser/operation-without-expression.fdml") {
      case Left(e: LatisException) =>
        assert(e.message.startsWith("latis-operation must contain expression in"))
      case _ => fail("")
    }
  }

  test("require that operations have valid expressions") {
    withFdmlFile("fdml-parser/operation-with-bad-expression.fdml") {
      case Left(e: LatisException) =>
        assert(e.message.startsWith("Failed to parse expression"))
      case _ => fail("")
    }
  }

  test("parse valid granule append FDML") {
    withFdmlFile("fdml-parser/granule-append.fdml") {
      case Left(err)   => fail(err.message)
      case Right(_: DatasetFdml) => fail("got DatasetFdml")
      case Right(GranuleAppendFdml(metadata, source, adapter, model, _)) =>
        assertEquals(
          metadata.properties,
          Map(
            "id" -> "granuleAppend",
            "class" -> "latis.dataset.GranuleAppendDataset"
          )
        )

        assertEquals(
          adapter,
          SingleAdapter("adapter-class", Map("class" -> "adapter-class"))
        )

        assertEquals(
          model,
          FFunction(
            FScalar(
              id"time",
              IntValueType,
              Map(
                "id" -> "time",
                "type" -> "int",
                "units" -> "days since 2000-01-01",
                "class" -> "latis.time.Time"
              )
            ),
            FScalar(
              id"a",
              IntValueType,
              Map("id" -> "a", "type" -> "int")
            ),
            Map.empty
          )
        )

        source match {
          case FdmlSource(DatasetFdml(_, source, adapter, model, _)) =>
            assertEquals(source, UriSource(new URI("file:///source")))

            assertEquals(
              model,
              FFunction(
                FScalar(
                  id"time",
                  IntValueType,
                  Map(
                    "id" -> "time",
                    "type" -> "int",
                    "units" -> "days since 2000-01-01",
                    "class" -> "latis.time.Time"
                  )
                ),
                FScalar(
                  id"uri",
                  StringValueType,
                  Map("id" -> "uri", "type" -> "string")
                ),
                Map.empty
              )
            )

            assertEquals(
              adapter,
              SingleAdapter("granule-class", Map("class" -> "granule-class"))
            )
        }
    }
  }

  private def withFdmlFile(uriStr: String)(f: Either[LatisException, Fdml] => Unit): Unit =
    NetUtils.resolveUri(uriStr) match {
      case Right(uri) => f(FdmlParser.parseUri(uri, false))
      case Left(err)  => fail(err.getMessage)
    }
}

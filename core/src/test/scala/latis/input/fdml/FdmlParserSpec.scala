package latis.input.fdml

import org.scalatest.EitherValues._
import org.scalatest.FlatSpec
import org.scalatest.Inside._
import org.scalatest.Matchers._

import latis.model.DoubleValueType
import latis.model.IntValueType
import latis.model.StringValueType
import latis.ops.parser.ast
import latis.util.LatisException
import latis.util.NetUtils

// FDML files live in core/src/test/resources/fdml-parser.

final class FdmlParserSpec extends FlatSpec {

  "An FDML parser" should "parse a valid FDML file" in
    withFdmlFile("fdml-parser/valid.fdml") { fdml =>
      inside(fdml.right.value) { case Fdml(metadata, source, adapter, model, operations) =>
        metadata.properties should contain ("id" -> "valid")
        metadata.properties should contain ("title" -> "Valid Dataset")

        inside(source) { case FSource(uri) =>
          uri.toString() should equal ("file:///fake/path")
        }

        inside(adapter) { case FAdapter(clss, attrs) =>
          clss should equal ("latis.input.TextAdapter")
          attrs should contain ("skipLines" -> "1")
        }

        // time -> (a, b, c) -> (d, e, f)

        inside(model) { case FFunction(domain, range, attrs) =>
          attrs should contain ("id" -> "outer")

          inside(domain) { case FScalar(id, ty, attrs) =>
            id should equal ("time")
            ty should equal (IntValueType)
            attrs should contain ("units" -> "days since 2000-01-01")
            attrs should contain ("class" -> "latis.time.Time")
          }

          inside(range) { case FFunction(domain, range, attrs) =>
            attrs should contain ("id" -> "inner")

            inside(domain) { case FTuple(fst, snd, rest, attrs) =>
              attrs should contain ("id" -> "inner_domain")

              inside(fst) { case FScalar(id, ty, attrs) =>
                id should equal ("a")
                ty should equal (IntValueType)
              }

              inside(snd) { case FScalar(id, ty, attrs) =>
                id should equal ("b")
                ty should equal (DoubleValueType)
              }

              rest should have length 1
              inside(rest) { case FScalar(id, ty, attrs) :: Nil =>
                id should equal ("c")
                ty should equal (StringValueType)
              }
            }

            inside(range) { case FTuple(fst, snd, rest, attrs) =>
              attrs should contain ("id" -> "inner_range")

              inside(fst) { case FScalar(id, ty, attrs) =>
                id should equal ("d")
                ty should equal (IntValueType)
              }

              inside(snd) { case FScalar(id, ty, attrs) =>
                id should equal ("e")
                ty should equal (DoubleValueType)
              }

              rest should have length 1
              inside(rest) { case FScalar(id, ty, attrs) :: Nil =>
                id should equal ("f")
                ty should equal (StringValueType)
              }
            }
          }
        }

        inside(operations) { case firstOp :: secondOp :: thirdOp :: Nil =>
          firstOp should equal (ast.Selection("time", ast.Gt, "2000-01-01"))
          secondOp should equal (ast.Projection(List("a", "b", "c")))
          thirdOp should equal (ast.Operation("rename", List("Constantinople", "Istanbul")))
        }
      }
    }

  it should "gracefully handle non-FDML files" in
    withFdmlFile("fdml-parser/not-fdml.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting dataset element")
    }

  it should "require a source element" in
    withFdmlFile("fdml-parser/no-source.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting source element")
    }

  it should "require a single source element" in
    withFdmlFile("fdml-parser/multiple-sources.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting a single source")
    }

  it should "require that sources have a URI attribute" in
    withFdmlFile("fdml-parser/no-source-uri.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting source with uri attribute")
    }

  it should "require a valid source URI" in
    withFdmlFile("fdml-parser/invalid-source-uri.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Source URI is malformed")
    }

  it should "require an adapter element" in
    withFdmlFile("fdml-parser/no-adapter.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting adapter element")
    }

  it should "require a single adapter element" in
    withFdmlFile("fdml-parser/multiple-adapters.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting a single adapter")
    }

  it should "require that adapters have a class attribute" in
    withFdmlFile("fdml-parser/no-adapter-class.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting adapter with class attribute")
    }

  it should "require a model" in
    withFdmlFile("fdml-parser/no-model.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting model starting with function")
    }

  it should "require that models start with functions" in
    withFdmlFile("fdml-parser/model-not-starting-with-function.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting model starting with function")
    }

  it should "require that functions have a domain and range" in
    withFdmlFile("fdml-parser/function-without-range.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting domain and range")
    }

  it should "require a single root function" in
    withFdmlFile("fdml-parser/multiple-root-functions.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting a single root function")
    }

  it should "require that tuples have at least two children" in
    withFdmlFile("fdml-parser/one-element-tuple.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting at least two children")
    }

  it should "require that scalars have identifiers" in
    withFdmlFile("fdml-parser/scalar-without-identifier.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting scalar with id attribute")
    }

  it should "require that scalars have types" in
    withFdmlFile("fdml-parser/scalar-without-type.fdml") { fdml =>
      fdml.left.value.getMessage should equal ("Expecting scalar with type attribute")
    }

  it should "require that scalars have valid types" in
    withFdmlFile("fdml-parser/scalar-without-valid-type.fdml") { fdml =>
      fdml.left.value.getMessage should startWith ("Invalid Scalar value type")
    }

  it should "require that operations have expressions" in
    withFdmlFile("fdml-parser/operation-without-expression.fdml") { fdml =>
      fdml.left.value.getMessage should startWith ("latis-operation must contain expression in")
    }

  it should "require that operations have valid expressions" in
    withFdmlFile("fdml-parser/operation-with-bad-expression.fdml") { fdml =>
      fdml.left.value.getMessage should startWith ("Failed to parse expression")
    }

  private def withFdmlFile(uriStr: String)(f: Either[LatisException, Fdml] => Any): Any =
    NetUtils.resolveUri(uriStr) match {
      case Right(uri) => f(FdmlParser.parseUri(uri, false))
      case Left(err)  => cancel(err.getMessage)
    }
}

package latis.input.fdml

import java.net.URI
import java.net.URISyntaxException

import scala.util.matching.Regex
import scala.xml._

import atto.Atto._
import atto._
import cats.implicits._

import latis.metadata.Metadata
import latis.model.ValueType
import latis.ops.parser.ast.CExpr
import latis.ops.parser.parsers.subexpression
import latis.util.FdmlUtils
import latis.util.LatisException
import latis.util.NetUtils

/** Provides methods for parsing things as FDML. */
object FdmlParser {

  /** Parses (and optionally validates) XML from a URI as FDML. */
  def parseUri(
    uri: URI,
    validate: Boolean = false
  ): Either[LatisException, Fdml] = for {
    _      <- FdmlUtils.validateFdml(uri).whenA(validate)
    xmlStr <- NetUtils.readUriIntoString(uri)
    fdml   <- parseXml(XML.loadString(xmlStr))
  } yield fdml

  /** Parses FDML from XML. */
  def parseXml(xml: Elem): Either[LatisException, Fdml] = for {
    _    <- isFdml(xml)
    fdml <- if (isGranuleAppendFdml(xml)) {
      parseGranuleAppendFdml(xml)
    } else {
      parseDatasetFdml(xml)
    }
  } yield fdml

  // Assuming that this is an FDML file if the root element is
  // <dataset>.
  private def isFdml(xml: Elem): Either[LatisException, Unit] =
    if (xml.label != "dataset") {
      LatisException("Expecting dataset element").asLeft
    } else ().asRight

  // Assuming that this in a granule append FDML file if the "source"
  // element has a "granules" element.
  private def isGranuleAppendFdml(xml: Elem): Boolean =
    (xml \ "source" \ "granules").nonEmpty

  private def parseDatasetFdml(
    xml: Elem
  ): Either[LatisException, DatasetFdml] = for {
    metadata   <- parseMetadata(xml).asRight
    source     <- parseUriSource(xml)
    adapter    <- parseAdapter(xml)
    function   <- findRootFunction(xml)
    model      <- parseFunction(function)
    exprs      <- parseProcessingInstructions(xml)
    operations <- parseExpressions(exprs)
  } yield DatasetFdml(metadata, source, adapter, model, operations)

  private def parseGranuleAppendFdml(
    xml: Elem
  ): Either[LatisException, GranuleAppendFdml] = for {
    metadata <- parseMetadata(xml).asRight
    source   <- parseGranuleSource(xml)
    template <- parseTemplate(xml)
  } yield GranuleAppendFdml(metadata, source, template)

  private def parseMetadata(xml: Elem): Metadata =
    Metadata(xml.attributes.asAttrMap)

  private def parseUriSource(xml: Elem): Either[LatisException, UriSource] =
    (xml \ "source").toList match {
      case elem :: Nil => for {
        attrs  <- elem.attributes.asAttrMap.asRight
        uriStr <- attrs.get("uri").toRight {
          LatisException("Expecting source with uri attribute")
        }
        uri    <- Either.catchOnly[URISyntaxException] {
          new URI(uriStr)
        }.leftMap(LatisException("Source URI is malformed", _))
      } yield UriSource(uri)
      case _ :: _ => LatisException("Expecting a single source").asLeft
      case _      => LatisException("Expecting source element").asLeft
    }

  private def parseGranuleSource(
    xml: Elem
  ): Either[LatisException, GranuleSource] =
    (xml \ "source").toList match {
      case elem :: Nil => (elem \ "granules").toList match {
        case (elem: Elem) :: Nil => parseDatasetFdml(elem).map(GranuleSource(_))
        case _ :: _ => LatisException("Expecting a single granules element").asLeft
        case _      => LatisException("Expecting a granules element").asLeft
      }
      case _ :: _ => LatisException("Expecting a single source").asLeft
      case _      => LatisException("Expecting source element").asLeft
    }

  private def parseAdapter(xml: Elem): Either[LatisException, FAdapter] =
    (xml \ "adapter").toList match {
      case elem :: Nil => for {
        attrs <- elem.attributes.asAttrMap.asRight
        clss  <- attrs.get("class").toRight {
          LatisException("Expecting adapter with class attribute")
        }
      } yield FAdapter(clss, attrs)
      case _ :: _ => LatisException("Expecting a single adapter").asLeft
      case _      => LatisException("Expecting adapter element").asLeft
    }

  private def parseTemplate(xml: Elem): Either[LatisException, FTemplate] =
    (xml \ "template").toList match {
      case (elem: Elem) :: Nil => for {
        adapter  <- parseAdapter(elem)
        function <- findRootFunction(elem)
        model    <- parseFunction(function)
      } yield FTemplate(adapter, model)
      case _ :: _ => LatisException("Expecting a single template").asLeft
      case _      => LatisException("Expecting a template element").asLeft
    }

  private def findRootFunction(xml: Elem): Either[LatisException, Node] =
    (xml \ "function").toList match {
      case elem :: Nil => elem.asRight
      case _    :: _   => LatisException("Expecting a single root function").asLeft
      case _           => LatisException("Expecting model starting with function").asLeft
    }

  private def parseModel(node: Node): Either[LatisException, FModel] =
    node.label match {
      case "function" => parseFunction(node)
      case "tuple"    => parseTuple(node)
      case "scalar"   => parseScalar(node)
      case _          =>
        LatisException("Expecting function, tuple, or scalar element").asLeft
    }

  private def isModelNode(node: Node): Boolean =
    node.label == "function" || node.label == "scalar" || node.label == "tuple"

  private def parseFunction(node: Node): Either[LatisException, FFunction] =
    node.toList match {
      case f :: Nil => f.child.filter(isModelNode) match {
        case d :: r :: Nil => for {
          attrs  <- f.attributes.asAttrMap.asRight
          domain <- parseModel(d)
          range  <- parseModel(r)
        } yield FFunction(domain, range, attrs)
        case _ => LatisException("Expecting domain and range").asLeft
      }
      case _ => LatisException("Expecting a single function").asLeft
    }

  private def parseTuple(node: Node): Either[LatisException, FTuple] =
    node.toList match {
      case t :: Nil => t.child.filter(isModelNode) match {
        case a :: b :: rest => for {
          attrs <- t.attributes.asAttrMap.asRight
          fst   <- parseModel(a)
          snd   <- parseModel(b)
          more  <- rest.traverse(parseModel)
        } yield FTuple(fst, snd, more, attrs)
        case _ => LatisException("Expecting at least two children").asLeft
      }
      case _ => LatisException("Expecting a single tuple").asLeft
    }

  private def parseScalar(node: Node): Either[LatisException, FScalar] =
    node.toList match {
      case s :: Nil => for {
        attrs <- s.attributes.asAttrMap.asRight
        id    <- attrs.get("id").toRight {
          LatisException("Expecting scalar with id attribute")
        }
        tyStr <- attrs.get("type").toRight {
          LatisException("Expecting scalar with type attribute")
        }
        ty    <- ValueType.fromName(tyStr)
      } yield FScalar(id, ty, attrs)
      case _ => LatisException("Expecting a single scalar").asLeft
    }

  private def parseProcessingInstructions(xml: Elem): Either[LatisException, List[String]] = {
    val expr: Regex = "expression=\"(.*)\"".r
    xml.child.collect {
      case ProcInstr("latis-operation", expr(v)) => v.asRight
      case ProcInstr("latis-operation", s) =>
        LatisException(s"latis-operation must contain expression in $s").asLeft
    }.toList.sequence
  }

  private def parseExpressions(
    expressions: List[String]
  ): Either[LatisException, List[CExpr]] =
    expressions.traverse(parseExpression)

  private def parseExpression(
    expression: String
  ): Either[LatisException, CExpr] =
    subexpression.parseOnly(expression) match {
      case ParseResult.Done(_, e) => Right(e)
      case _                      => Left(LatisException(s"Failed to parse expression $expression"))
    }
}

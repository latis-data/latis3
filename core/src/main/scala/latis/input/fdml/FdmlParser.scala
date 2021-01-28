package latis.input.fdml

import java.net.URI
import java.net.URISyntaxException

import scala.util.matching.Regex
import scala.xml._

import atto.Atto._
import atto._
import cats.syntax.all._

import latis.metadata.Metadata
import latis.model.ValueType
import latis.util.FdmlUtils
import latis.util.Identifier
import latis.util.LatisException
import latis.util.NetUtils
import latis.util.dap2.parser.ast.CExpr
import latis.util.dap2.parser.parsers.subexpression

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
  // element has a "dataset" element.
  private def isGranuleAppendFdml(xml: Elem): Boolean =
    (xml \ "source" \ "dataset").nonEmpty

  private def parseDatasetFdml(
    xml: Elem
  ): Either[LatisException, DatasetFdml] = for {
    metadata   <- parseMetadata(xml).asRight
    source     <- parseUriSource(xml)
    adapter    <- parseSingleAdapter(xml)
    function   <- findRootFunction(xml)
    model      <- parseFunction(function)
    exprs      <- parseProcessingInstructions(xml)
    operations <- exprs.traverse(parseExpression)
  } yield DatasetFdml(metadata, source, adapter, model, operations)

  private def parseGranuleAppendFdml(
    xml: Elem
  ): Either[LatisException, GranuleAppendFdml] = for {
    metadata   <- parseMetadata(xml).asRight
    source     <- parseFdmlSource(xml)
    adapter    <- parseNestedAdapter(xml)
    function   <- findRootFunction(xml)
    model      <- parseFunction(function)
    exprs      <- parseProcessingInstructions(xml)
    operations <- exprs.traverse(parseExpression)
  } yield GranuleAppendFdml(metadata, source, adapter, model, operations)

  private def parseMetadata(xml: Elem): Metadata = {
    val md = xml.attributes.asAttrMap.filter {
      case ("xsi:noNamespaceSchemaLocation", _) => false
      case _ => true
    }
    Metadata(md)
  }

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

  private def parseFdmlSource(
    xml: Elem
  ): Either[LatisException, FdmlSource] =
    (xml \ "source").toList match {
      case elem :: Nil => (elem \ "dataset").toList match {
        case (elem: Elem) :: Nil => parseDatasetFdml(elem).map(FdmlSource(_))
        case _ :: _ => LatisException("Expecting a single dataset element").asLeft
        case _      => LatisException("Expecting a dataset element").asLeft
      }
      case _ :: _ => LatisException("Expecting a single source").asLeft
      case _      => LatisException("Expecting source element").asLeft
    }

  private def parseSingleAdapter(
    xml: Elem
  ): Either[LatisException, SingleAdapter] =
    (xml \ "adapter").toList match {
      case elem :: Nil => for {
        attrs <- elem.attributes.asAttrMap.asRight
        clss  <- attrs.get("class").toRight {
          LatisException("Expecting adapter with class attribute")
        }
      } yield SingleAdapter(clss, attrs)
      case _ :: _ => LatisException("Expecting a single adapter").asLeft
      case _      => LatisException("Expecting adapter element").asLeft
    }

  private def parseNestedAdapter(
    xml: Elem
  ): Either[LatisException, NestedAdapter] =
    (xml \ "adapter").toList match {
      case (outer: Elem) :: Nil =>
        for {
          outerAdapter <- parseSingleAdapter(xml)
          innerAdapter <- parseSingleAdapter(outer)
        } yield NestedAdapter(outerAdapter.clss, outerAdapter.attributes, innerAdapter)
      case _ :: _ => LatisException("Expecting a single nested adapter element").asLeft
      case _      => LatisException("Expecting a nested adapter element").asLeft
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
        ident <- Identifier.fromString(id).toRight {
          LatisException(s"Scalar id '$id' is not a valid identifier")
        }
        tyStr <- attrs.get("type").toRight {
          LatisException("Expecting scalar with type attribute")
        }
        ty    <- ValueType.fromName(tyStr)
      } yield FScalar(ident, ty, attrs)
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

  private def parseExpression(
    expression: String
  ): Either[LatisException, CExpr] =
    subexpression.parseOnly(expression) match {
      case ParseResult.Done(_, e) => Right(e)
      case _                      => Left(LatisException(s"Failed to parse expression $expression"))
    }
}

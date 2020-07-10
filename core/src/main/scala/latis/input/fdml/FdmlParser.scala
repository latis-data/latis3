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
    _          <- isFdml(xml)
    metadata   <- parseMetadata(xml).asRight
    source     <- parseSource(xml)
    adapter    <- parseAdapter(xml)
    function   <- findRootFunction(xml)
    model      <- parseFunction(function)
    exprs      <- parseProcessingInstructions(xml)
    operations <- parseExpressions(exprs)
  } yield Fdml(metadata, source, adapter, model, operations)

  // Assuming that this is an FDML file if the root element is
  // <dataset>.
  private def isFdml(xml: Elem): Either[LatisException, Unit] =
    if (xml.label != "dataset") {
      LatisException("Expecting dataset element").asLeft
    } else ().asRight

  private def parseMetadata(xml: Elem): Metadata = {
    val md = xml.attributes.asAttrMap.filter {
      case ("xsi:noNamespaceSchemaLocation", _) => false
      case _ => true
    }
    Metadata(md)
  }

  private def parseSource(xml: Elem): Either[LatisException, FSource] =
    (xml \ "source").toList match {
      case elem :: Nil => for {
        attrs  <- elem.attributes.asAttrMap.asRight
        uriStr <- attrs.get("uri").toRight {
          LatisException("Expecting source with uri attribute")
        }
        uri    <- Either.catchOnly[URISyntaxException] {
          new URI(uriStr)
        }.leftMap(LatisException("Source URI is malformed", _))
      } yield FSource(uri)
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

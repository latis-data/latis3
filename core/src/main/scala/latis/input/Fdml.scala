package latis.input

import java.net.URI

import scala.collection.immutable.Map
import scala.xml._

import latis.metadata.Metadata
import latis.model._
import latis.util.ReflectionUtils

class Fdml(xml: Elem, cl: ClassLoader) {

  // Defines global metadata
  def metadata: Metadata = Metadata(getAttributes(xml))
  //TODO: require id?

  // Defines the source of the dataset as a URI
  val locationUri: String = (xml \ "source" \ "@uri").text
  def uri: URI = new URI(locationUri)

  // Defines the Adapter for reading the data
  def adapter: Adapter = createAdapter(xml \ "adapter", model)

  // Defines the model of the dataset
  def model: DataType = createModel(xml \ "function").get

  /**
   * Recursively parse the function XML element into a domain and range.
   * The createDataType function introduces recursion because it calls
   * functions that can also call createDataType.
   */
  def createModel(functionNode: NodeSeq): Option[DataType] =
    if (functionNode.length > 0) {
      createFunctionDataType(functionNode)
    } else {
      None
    }

  /**
   * The concept of the datatype exists in the fdml.xsd schema.
   */
  def isDatatype(node: Node): Boolean =
    node.label == "function" ||
      node.label == "scalar" ||
      node.label == "tuple"

  /**
   * Create a DataType object from XML.
   * Create domain and range objects with the same code.
   */
  def createDataType(datatype: Node): DataType =
    if (datatype.label == "function") createFunctionDataType(datatype).get
    else if (datatype.label == "scalar") createScalarDataType(datatype).get
    else createTupleDataType(datatype).get

  /**
   * Create an Adapter object from XML and the supplied model.
   */
  def createAdapter(adapterNode: NodeSeq, model: DataType): Adapter = {
    val properties: Seq[(String, String)] = getAttributes(adapterNode).toSeq
    val config = AdapterConfig(properties: _*)
    ReflectionUtils
      .callMethodOnCompanionObject(
        cl,
        "latis.input.AdapterFactory",
        "makeAdapter",
        model,
        config,
        cl
      )
      .asInstanceOf[Adapter]
  }

  /**
   * Function datatypes must contain a domain and a range.
   */
  def createFunctionDataType(functionNodes: NodeSeq): Option[Function] =
    if (functionNodes.length > 0) {
      val functionNode: Node = functionNodes.head
      val attributes: Map[String, String] = getAttributes(functionNodes)
      val datatypes: Seq[Node] = functionNode.child.filter(isDatatype)
      val domain: Node = datatypes.head
      val range: Node = datatypes.tail.head
      if (attributes.isEmpty) {
        Some(Function(createDataType(domain), createDataType(range)))
      } else {
        Some(
          Function(
            Metadata(attributes),
            createDataType(domain),
            createDataType(range)
          )
        )
      }
    } else {
      None
    }

  /**
   * Scalar datatypes only contain metadata.
   */
  def createScalarDataType(scalarNode: Node): Option[Scalar] =
    if (scalarNode.nonEmpty) {
      val (classMap, mdMap) = getAttributes(scalarNode).partition(_._1 == "class")
      val md = Metadata(mdMap)
      if (classMap.nonEmpty) {
        val s = ReflectionUtils.callMethodOnCompanionObject(
          cl,
          classMap("class"),
          "apply",
          md
        )
        Option(s.asInstanceOf[Scalar])
      } else Option(Scalar(md))
    } else {
      None
    }

  /**
   * Tuple datatypes can contain an arbitrary number of elements.
   */
  def createTupleDataType(tupleNode: Node): Option[Tuple] = {
    val attributes: Map[String, String] = getAttributes(tupleNode)
    val tupleElements: Seq[Node] = tupleNode.child.filter(isDatatype)
    val tuple: List[DataType] = tupleElements.map(createDataType).toList
    if (tuple.nonEmpty) {
      if (attributes.isEmpty) {
        Some(Tuple(tuple))
      } else {
        Some(Tuple(Metadata(attributes), tuple))
      }
    } else {
      None
    }
  }

  /**
   * Get the value of this element's attribute with the given name.
   */
  def getAttribute(xml: NodeSeq, name: String): Option[String] =
    (xml \ ("@" + name)).text match {
      case s: String if s.length > 0 => Some(s)
      case _                         => None
    }

  /**
   * Get all of the attributes for the specified XML Node.
   */
  def getAttributes(xml: NodeSeq): Map[String, String] = {
    val node: Node = xml.head
    val seq: Iterable[(String, String)] = for {
      att <- node.attributes
    } yield (att.key, att.value.text)
    seq.toMap
  }

}

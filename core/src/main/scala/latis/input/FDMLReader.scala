package latis.input

import scala.xml._

import scala.collection.immutable.Map

import latis.metadata.Metadata
import latis.model._
import latis.input._
import java.net.URI

/**
 * From an FDML file an FDMLReader reader creates a dataset, configures it's adapter, and builds the dataset's model.
 */
class FDMLReader {
  
}

object FDMLReader {
  
  /**
   * Convert a string containing valid XML to an XML element.
   */
  def load(xmlText: String): Elem = XML.loadString(xmlText)
  
  /** Recursively parse the function XML element into a domain and range.
   *The createDataType function introduces recursion because it calls functions that can also call createDataType.
   */
  def createModel(functionNode: Node): DataType = {
    createFunctionDataType(functionNode)
  }
  
  /**
   * The concept of the datatype exists in the fdml.xsd schema.
   */
  def isDatatype(node: Node): Boolean = {
    node.label == "function" ||
    node.label == "scalar"  ||
    node.label == "tuple"
  }
  
  /**
   * Create a DataType object from XML.
   * Create domain and range objects with the same code.
   */
  def createDataType(datatype: Node): DataType = {
    if (datatype.label == "function") createFunctionDataType(datatype) 
    else if (datatype.label == "scalar") createScalarDataType(datatype)
    else createTupleDataType(datatype)
  }
  
  /**
   * Create an Adapter object from XML and the supplied model.
   */
  def createAdapter(adapterNode: Node, model: DataType): Adapter = {
    val adapterClass: Option[String] = getAttribute(adapterNode, "class")
    val attributes: Seq[(String, String)] = getAttributes(adapterNode).filter(_._1 != "class").toSeq
    val config = AdapterFactory(adapterClass, attributes: _*)
    
    // Reflection is the preferred way to create adapters, but I can't get this to work
    // TODO: this is an unsafe get
    //val adapterConfig = Class.forName(adapterClass.get).newInstance().asInstanceOf[Adapter].Config(attributes: _*)
    
    AdapterFactory(adapterClass, config, model)
  }
  
  /**
   * Function datatypes must contain a domain and a range.
   */
  def createFunctionDataType(functionNode: Node): Function = {
    val attributes: Map[String, String] = getAttributes(functionNode)   
    val datatypes: Seq[Node] = functionNode.child.filter(isDatatype(_))
    val domain: Node  = datatypes.head
    val range: Node = datatypes.tail.head
    if (attributes.isEmpty) {
      Function(createDataType(domain), createDataType(range))
    } else {
      Function(Metadata(attributes), createDataType(domain), createDataType(range))
    }
  }
  
  /**
   * Scalar datatypes only contain metadata.
   */
  def createScalarDataType(scalarNode: Node): Scalar = {
    Scalar(Metadata(getAttributes(scalarNode)))
  }
  
  /**
   * Tuple datatypes can contain an arbitrary number of elements.
   */
  def createTupleDataType(tupleNode:Node): Tuple = {
    val attributes: Map[String, String] = getAttributes(tupleNode) 
    val tupleElements: Seq[Node] = tupleNode.child.filter(isDatatype(_))
    val tuple: List[DataType] = tupleElements.map(createDataType(_)).toList
    if (attributes.isEmpty) {
      Tuple(tuple: _*)
    } else {
      Tuple(Metadata(attributes), tuple: _*)
    }
  }
  
  /**
   * Get the value of this element's attribute with the given name.
   */
  def getAttribute(xml: Node, name: String): Option[String] = {
    (xml \ ("@"+name)).text match {
      case s: String if s.length > 0 => Some(s)
      case _ => None
    }
  }
  
  /**
   * Get all of the attributes for the specified XML Node.
   */
  def getAttributes(xml: Node): Map[String, String] = {
    val seq = for (
      att <- xml.attributes
    ) yield (att.key, att.value.text)
    Map[String, String](seq.toList: _*)
  }
  
  /**
   * Parse an FDML file and create a dataset.
   */
  def parse(xml: Elem): Option[Dataset] = {
    if ( xml.label == "dataset" ) {
      val datasetName = (xml \ "@name").text
      val datasetUri = (xml \ "@uri").text
      val adapterNode: Node = (xml \ "adapter").head      // convert a NodeSeq to a Node
      val functionNode: Node = (xml \ "function").head    // convert a NodeSeq to a Node
      if ( functionNode.length > 0 ) {
        val source = new AdaptedDatasetSource {
          def uri: URI = new URI(datasetUri)
          def metaData: Metadata = Metadata("name" -> datasetName, "id" -> uri.getPath) 
          val model: DataType = createModel(functionNode)
          def adapter: Adapter = createAdapter(adapterNode, model)
        }
        Some(source.getDataset()) 
      } else {
        None
      }
    } else {
      None
    }
  }
}
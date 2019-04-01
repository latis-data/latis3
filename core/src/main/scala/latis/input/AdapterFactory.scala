package latis.input

import latis.input._
import latis.model._

/**
 * Class creates objects and classes based on the class name.
 */
class AdapterFactory {
  
}

object AdapterFactory {
  def apply(className: Option[String], config: TextAdapter.Config, model: DataType): Adapter  = className.getOrElse("") match {
    case "latis.input.TextAdapter" => 
      new TextAdapter(config, model) 
    // TODO: error handling strategy needs to be defined
    case _                         => 
      val cls = Class.forName(className.get) //TODO: handle ClassNotFoundException?
      cls.getConstructor().newInstance().asInstanceOf[Adapter]
    //  new RuntimeException("Non-existant classname was specified in FDML file for Adapter.")
  }
  
  def apply(className: Option[String], arguments: (String, String)*) = className.getOrElse("") match {
    case "latis.input.TextAdapter" => 
      TextAdapter.Config(arguments: _*)
    // TODO: error handling strategy needs to be defined
    case _  => null
    //  new RuntimeException("Non-existant classname was specified in FDML file for Adapter.")
  }
}
package latis.input

import latis.input._
import latis.model._

/**
 * Class creates objects and classes based on the class name.
 */
class AdapterFactory {
  
}

object AdapterFactory {
  def apply(className: Option[String], config: TextAdapter.Config, model: DataType)  = className.getOrElse("") match {
    case "latis.input.TextAdapter" => 
      new TextAdapter(config, model) 
    // TODO: error handling strategy needs to be defined
    //case _                         => 
    //  new RuntimeException("Non-existant classname was specified in FDML file for Adapter.")
  }
  
  def apply(className: Option[String], arguments: (String, String)*) = className.getOrElse("") match {
    case "latis.input.TextAdapter" => 
      TextAdapter.Config(arguments: _*)
    // TODO: error handling strategy needs to be defined
    //case _                         => 
    //  new RuntimeException("Non-existant classname was specified in FDML file for Adapter.")
  }
}
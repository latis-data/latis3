package latis.input

import latis.model.Dataset
import latis.ops.Operation

/**
 * A DatasetSource is a provider of a LaTiS Dataset
 * as opposed to an Adapter that just provides data.
 * Provided Operations should be applied lazily to the Dataset.
 */
trait DatasetSource {
  
  /**
   * Return the Dataset with the given Operations applied to it.
   */
  def getDataset(operations: Seq[Operation]): Dataset
  
  /**
   * Return the Dataset with no Operations applied to it.
   */
  def getDataset(): Dataset = getDataset(Seq.empty)
  
}

object DatasetSource {
  
  /**
   * Get the DatasetSource for a given dataset by its identifier.
   */
//  def fromName(datasetId: String): DatasetSource = {
//    //Look for a matching "reader" property.
//    LatisProperties.get(s"reader.${datasetId}.class") match {
//      case Some(s) =>
//        Class.forName(s).getConstructor().newInstance().asInstanceOf[DatasetSource]
//    }
//    
////    //TODO: add other sources: tsml. lemr, properties
////    //import scala.reflect.runtime.currentMirror
////    val ru = scala.reflect.runtime.universe
////    val mirror = ru.runtimeMirror(getClass.getClassLoader)
////    
////    val cls = LatisProperties.getOrElse("dataset.dir", "datasets") + "." + dsid
////    
////    val moduleSymbol = mirror.classSymbol(Class.forName(cls)).companion.asModule
////    mirror.reflectModule(moduleSymbol).instance.asInstanceOf[DatasetDescriptor]
//  }
}